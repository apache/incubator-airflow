# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Serialized DAG and BaseOperator"""
import datetime
import enum
import logging
from inspect import Parameter, signature
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set, Union

import cattr
import pendulum
from dateutil import relativedelta

try:
    from functools import cache
except ImportError:
    from functools import lru_cache

    cache = lru_cache(maxsize=None)
from pendulum.tz.timezone import Timezone

from airflow.exceptions import AirflowException, SerializationError
from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
from airflow.models.connection import Connection
from airflow.models.dag import DAG
from airflow.providers_manager import ProvidersManager
from airflow.serialization.enums import DagAttributeTypes as DAT, Encoding
from airflow.serialization.helpers import serialize_template_field
from airflow.serialization.json_schema import Validator, load_dag_schema
from airflow.settings import json
from airflow.utils.code_utils import get_python_source
from airflow.utils.module_loading import import_string
from airflow.utils.task_group import TaskGroup

try:
    # isort: off
    from kubernetes.client import models as k8s
    from airflow.kubernetes.pod_generator import PodGenerator

    # isort: on
    HAS_KUBERNETES = True
except ImportError:
    HAS_KUBERNETES = False


if TYPE_CHECKING:
    from airflow.ti_deps.deps.base_ti_dep import BaseTIDep


log = logging.getLogger(__name__)

_OPERATOR_EXTRA_LINKS: Set[str] = {
    "airflow.operators.trigger_dagrun.TriggerDagRunLink",
    "airflow.sensors.external_task.ExternalTaskSensorLink",
    # Deprecated names, so that existing serialized dags load straight away.
    "airflow.operators.dagrun_operator.TriggerDagRunLink",
    "airflow.sensors.external_task_sensor.ExternalTaskSensorLink",
}


@cache
def get_operator_extra_links():
    """
    Returns operator extra links - both the ones that are built in and the ones that come from
    the providers.

    :return: set of extra links
    """
    _OPERATOR_EXTRA_LINKS.update(ProvidersManager().extra_links_class_names)
    return _OPERATOR_EXTRA_LINKS


class BaseSerialization:
    """BaseSerialization provides utils for serialization."""

    # JSON primitive types.
    _primitive_types = (int, bool, float, str)

    # Time types.
    # datetime.date and datetime.time are converted to strings.
    _datetime_types = (datetime.datetime,)

    # Object types that are always excluded in serialization.
    _excluded_types = (logging.Logger, Connection, type)

    _json_schema: Optional[Validator] = None

    # Should the extra operator link be loaded via plugins when
    # de-serializing the DAG? This flag is set to False in Scheduler so that Extra Operator links
    # are not loaded to not run User code in Scheduler.
    _load_operator_extra_links = True

    _CONSTRUCTOR_PARAMS: Dict[str, Parameter] = {}

    SERIALIZER_VERSION = 1

    @classmethod
    def to_json(cls, var: Union[DAG, BaseOperator, dict, list, set, tuple]) -> str:
        """Stringifies DAGs and operators contained by var and returns a JSON string of var."""
        return json.dumps(cls.to_dict(var), ensure_ascii=True)

    @classmethod
    def to_dict(cls, var: Union[DAG, BaseOperator, dict, list, set, tuple]) -> dict:
        """Stringifies DAGs and operators contained by var and returns a dict of var."""
        # Don't call on this class directly - only SerializedDAG or
        # SerializedBaseOperator should be used as the "entrypoint"
        raise NotImplementedError()

    @classmethod
    def from_json(cls, serialized_obj: str) -> Union['BaseSerialization', dict, list, set, tuple]:
        """Deserializes json_str and reconstructs all DAGs and operators it contains."""
        return cls.from_dict(json.loads(serialized_obj))

    @classmethod
    def from_dict(
        cls, serialized_obj: Dict[Encoding, Any]
    ) -> Union['BaseSerialization', dict, list, set, tuple]:
        """Deserializes a python dict stored with type decorators and
        reconstructs all DAGs and operators it contains.
        """
        return cls._deserialize(serialized_obj)

    @classmethod
    def validate_schema(cls, serialized_obj: Union[str, dict]) -> None:
        """Validate serialized_obj satisfies JSON schema."""
        if cls._json_schema is None:
            raise AirflowException(f'JSON schema of {cls.__name__:s} is not set.')

        if isinstance(serialized_obj, dict):
            cls._json_schema.validate(serialized_obj)
        elif isinstance(serialized_obj, str):
            cls._json_schema.validate(json.loads(serialized_obj))
        else:
            raise TypeError("Invalid type: Only dict and str are supported.")

    @staticmethod
    def _encode(x: Any, type_: Any) -> Dict[Encoding, Any]:
        """Encode data by a JSON dict."""
        return {Encoding.VAR: x, Encoding.TYPE: type_}

    @classmethod
    def _is_primitive(cls, var: Any) -> bool:
        """Primitive types."""
        return var is None or isinstance(var, cls._primitive_types)

    @classmethod
    def _is_excluded(cls, var: Any, attrname: str, instance: Any) -> bool:
        """Types excluded from serialization."""
        if var is None:
            if not cls._is_constructor_param(attrname, instance):
                # Any instance attribute, that is not a constructor argument, we exclude None as the default
                return True

            return cls._value_is_hardcoded_default(attrname, var, instance)
        return isinstance(var, cls._excluded_types) or cls._value_is_hardcoded_default(
            attrname, var, instance
        )

    @classmethod
    def serialize_to_json(
        cls, object_to_serialize: Union[BaseOperator, DAG], decorated_fields: Set
    ) -> Dict[str, Any]:
        """Serializes an object to json"""
        serialized_object: Dict[str, Any] = {}
        keys_to_serialize = object_to_serialize.get_serialized_fields()
        for key in keys_to_serialize:
            # None is ignored in serialized form and is added back in deserialization.
            value = getattr(object_to_serialize, key, None)
            if cls._is_excluded(value, key, object_to_serialize):
                continue

            if key in decorated_fields:
                serialized_object[key] = cls._serialize(value)
            else:
                value = cls._serialize(value)
                if isinstance(value, dict) and "__type" in value:
                    value = value["__var"]
                serialized_object[key] = value
        return serialized_object

    # pylint: disable=too-many-return-statements
    @classmethod
    def _serialize(cls, var: Any) -> Any:  # Unfortunately there is no support for recursive types in mypy
        """Helper function of depth first search for serialization.

        The serialization protocol is:

        (1) keeping JSON supported types: primitives, dict, list;
        (2) encoding other types as ``{TYPE: 'foo', VAR: 'bar'}``, the deserialization
            step decode VAR according to TYPE;
        (3) Operator has a special field CLASS to record the original class
            name for displaying in UI.
        """
        if cls._is_primitive(var):
            # enum.IntEnum is an int instance, it causes json dumps error so we use its value.
            if isinstance(var, enum.Enum):
                return var.value
            return var
        elif isinstance(var, dict):
            return cls._encode({str(k): cls._serialize(v) for k, v in var.items()}, type_=DAT.DICT)
        elif isinstance(var, list):
            return sorted(cls._serialize(v) for v in var)
        elif HAS_KUBERNETES and isinstance(var, k8s.V1Pod):
            json_pod = PodGenerator.serialize_pod(var)
            return cls._encode(json_pod, type_=DAT.POD)
        elif isinstance(var, DAG):
            return SerializedDAG.serialize_dag(var)
        elif isinstance(var, BaseOperator):
            return SerializedBaseOperator.serialize_operator(var)
        elif isinstance(var, cls._datetime_types):
            return cls._encode(var.timestamp(), type_=DAT.DATETIME)
        elif isinstance(var, datetime.timedelta):
            return cls._encode(var.total_seconds(), type_=DAT.TIMEDELTA)
        elif isinstance(var, Timezone):
            return cls._encode(str(var.name), type_=DAT.TIMEZONE)
        elif isinstance(var, relativedelta.relativedelta):
            encoded = {k: v for k, v in var.__dict__.items() if not k.startswith("_") and v}
            if var.weekday and var.weekday.n:
                # Every n'th Friday for example
                encoded['weekday'] = [var.weekday.weekday, var.weekday.n]
            elif var.weekday:
                encoded['weekday'] = [var.weekday.weekday]
            return cls._encode(encoded, type_=DAT.RELATIVEDELTA)
        elif callable(var):
            return str(get_python_source(var))
        elif isinstance(var, set):
            # FIXME: casts set to list in customized serialization in future.
            return cls._encode(sorted(cls._serialize(v) for v in var), type_=DAT.SET)
        elif isinstance(var, tuple):
            # FIXME: casts tuple to list in customized serialization in future.
            return cls._encode(sorted(cls._serialize(v) for v in var), type_=DAT.TUPLE)
        elif isinstance(var, TaskGroup):
            return SerializedTaskGroup.serialize_task_group(var)
        else:
            log.debug('Cast type %s to str in serialization.', type(var))
            return str(var)

    # pylint: enable=too-many-return-statements

    @classmethod
    def _deserialize(cls, encoded_var: Any) -> Any:  # pylint: disable=too-many-return-statements
        """Helper function of depth first search for deserialization."""
        # JSON primitives (except for dict) are not encoded.
        if cls._is_primitive(encoded_var):
            return encoded_var
        elif isinstance(encoded_var, list):
            return [cls._deserialize(v) for v in encoded_var]

        if not isinstance(encoded_var, dict):
            raise ValueError(f"The encoded_var should be dict and is {type(encoded_var)}")
        var = encoded_var[Encoding.VAR]
        type_ = encoded_var[Encoding.TYPE]

        if type_ == DAT.DICT:
            return {k: cls._deserialize(v) for k, v in var.items()}
        elif type_ == DAT.DAG:
            return SerializedDAG.deserialize_dag(var)
        elif type_ == DAT.OP:
            return SerializedBaseOperator.deserialize_operator(var)
        elif type_ == DAT.DATETIME:
            return pendulum.from_timestamp(var)
        elif type_ == DAT.POD:
            if not HAS_KUBERNETES:
                raise RuntimeError("Cannot deserialize POD objects without kubernetes libraries installed!")
            pod = PodGenerator.deserialize_model_dict(var)
            return pod
        elif type_ == DAT.TIMEDELTA:
            return datetime.timedelta(seconds=var)
        elif type_ == DAT.TIMEZONE:
            return Timezone(var)
        elif type_ == DAT.RELATIVEDELTA:
            if 'weekday' in var:
                var['weekday'] = relativedelta.weekday(*var['weekday'])  # type: ignore
            return relativedelta.relativedelta(**var)
        elif type_ == DAT.SET:
            return {cls._deserialize(v) for v in var}
        elif type_ == DAT.TUPLE:
            # pylint: disable=consider-using-generator
            return tuple([cls._deserialize(v) for v in var])
        else:
            raise TypeError(f'Invalid type {type_!s} in deserialization.')

    _deserialize_datetime = pendulum.from_timestamp
    _deserialize_timezone = pendulum.tz.timezone

    @classmethod
    def _deserialize_timedelta(cls, seconds: int) -> datetime.timedelta:
        return datetime.timedelta(seconds=seconds)

    @classmethod
    def _is_constructor_param(cls, attrname: str, instance: Any) -> bool:
        # pylint: disable=unused-argument
        return attrname in cls._CONSTRUCTOR_PARAMS

    @classmethod
    def _value_is_hardcoded_default(cls, attrname: str, value: Any, instance: Any) -> bool:
        """
        Return true if ``value`` is the hard-coded default for the given attribute.

        This takes in to account cases where the ``concurrency`` parameter is
        stored in the ``_concurrency`` attribute.

        And by using `is` here only and not `==` this copes with the case a
        user explicitly specifies an attribute with the same "value" as the
        default. (This is because ``"default" is "default"`` will be False as
        they are different strings with the same characters.)

        Also returns True if the value is an empty list or empty dict. This is done
        to account for the case where the default value of the field is None but has the
        ``field = field or {}`` set.
        """
        # pylint: disable=unused-argument
        if attrname in cls._CONSTRUCTOR_PARAMS and (
            cls._CONSTRUCTOR_PARAMS[attrname] is value or (value in [{}, []])
        ):
            return True
        return False


class SerializedBaseOperator(BaseOperator, BaseSerialization):
    """A JSON serializable representation of operator.

    All operators are casted to SerializedBaseOperator after deserialization.
    Class specific attributes used by UI are move to object attributes.
    """

    _decorated_fields = {'executor_config'}

    _CONSTRUCTOR_PARAMS = {
        k: v.default
        for k, v in signature(BaseOperator.__init__).parameters.items()
        if v.default is not v.empty
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # task_type is used by UI to display the correct class type, because UI only
        # receives BaseOperator from deserialized DAGs.
        self._task_type = 'BaseOperator'
        # Move class attributes into object attributes.
        self.ui_color = BaseOperator.ui_color
        self.ui_fgcolor = BaseOperator.ui_fgcolor
        self.template_fields = BaseOperator.template_fields
        self.operator_extra_links = BaseOperator.operator_extra_links

    @property
    def task_type(self) -> str:
        # Overwrites task_type of BaseOperator to use _task_type instead of
        # __class__.__name__.
        return self._task_type

    @task_type.setter
    def task_type(self, task_type: str):
        self._task_type = task_type

    @classmethod
    def serialize_operator(cls, op: BaseOperator) -> Dict[str, Any]:
        """Serializes operator into a JSON object."""
        serialize_op = cls.serialize_to_json(op, cls._decorated_fields)
        serialize_op['_task_type'] = op.__class__.__name__
        serialize_op['_task_module'] = op.__class__.__module__

        # Used to determine if an Operator is inherited from DummyOperator
        serialize_op['_is_dummy'] = op.inherits_from_dummy_operator

        if op.operator_extra_links:
            serialize_op['_operator_extra_links'] = cls._serialize_operator_extra_links(
                op.operator_extra_links
            )

        if op.deps is not BaseOperator.deps:
            # Are the deps different to BaseOperator, if so serialize the class names!
            # For Airflow 2.0 expediency we _only_ allow built in Dep classes.
            # Fix this for 2.0.x or 2.1
            deps = []
            for dep in op.deps:
                klass = type(dep)
                module_name = klass.__module__
                if not module_name.startswith("airflow.ti_deps.deps."):
                    raise SerializationError(
                        f"Cannot serialize {(op.dag.dag_id + '.' + op.task_id)!r} with `deps` from non-core "
                        f"module {module_name!r}"
                    )

                deps.append(f'{module_name}.{klass.__name__}')
            serialize_op['deps'] = deps

        # Store all template_fields as they are if there are JSON Serializable
        # If not, store them as strings
        if op.template_fields:
            for template_field in op.template_fields:
                value = getattr(op, template_field, None)
                if not cls._is_excluded(value, template_field, op):
                    serialize_op[template_field] = serialize_template_field(value)

        return serialize_op

    @classmethod
    def deserialize_operator(cls, encoded_op: Dict[str, Any]) -> BaseOperator:
        """Deserializes an operator from a JSON object."""
        op = SerializedBaseOperator(task_id=encoded_op['task_id'])

        if "label" not in encoded_op:
            # Handle deserialization of old data before the introduction of TaskGroup
            encoded_op["label"] = encoded_op["task_id"]

        # Extra Operator Links defined in Plugins
        op_extra_links_from_plugin = {}

        # We don't want to load Extra Operator links in Scheduler
        if cls._load_operator_extra_links:  # pylint: disable=too-many-nested-blocks
            from airflow import plugins_manager

            plugins_manager.initialize_extra_operators_links_plugins()

            if plugins_manager.operator_extra_links is None:
                raise AirflowException("Can not load plugins")

            for ope in plugins_manager.operator_extra_links:
                for operator in ope.operators:
                    if (
                        operator.__name__ == encoded_op["_task_type"]
                        and operator.__module__ == encoded_op["_task_module"]
                    ):
                        op_extra_links_from_plugin.update({ope.name: ope})

            # If OperatorLinks are defined in Plugins but not in the Operator that is being Serialized
            # set the Operator links attribute
            # The case for "If OperatorLinks are defined in the operator that is being Serialized"
            # is handled in the deserialization loop where it matches k == "_operator_extra_links"
            if op_extra_links_from_plugin and "_operator_extra_links" not in encoded_op:
                setattr(op, "operator_extra_links", list(op_extra_links_from_plugin.values()))

        for k, v in encoded_op.items():

            if k == "_downstream_task_ids":
                v = set(v)
            elif k == "subdag":
                v = SerializedDAG.deserialize_dag(v)
            elif k in {"retry_delay", "execution_timeout", "sla", "max_retry_delay"}:
                v = cls._deserialize_timedelta(v)
            elif k in encoded_op["template_fields"]:
                pass
            elif k.endswith("_date"):
                v = cls._deserialize_datetime(v)
            elif k == "_operator_extra_links":
                if cls._load_operator_extra_links:
                    op_predefined_extra_links = cls._deserialize_operator_extra_links(v)

                    # If OperatorLinks with the same name exists, Links via Plugin have higher precedence
                    op_predefined_extra_links.update(op_extra_links_from_plugin)
                else:
                    op_predefined_extra_links = {}

                v = list(op_predefined_extra_links.values())
                k = "operator_extra_links"

            elif k == "deps":
                v = cls._deserialize_deps(v)
            elif k in cls._decorated_fields or k not in op.get_serialized_fields():
                v = cls._deserialize(v)
            # else use v as it is

            setattr(op, k, v)

        for k in op.get_serialized_fields() - encoded_op.keys() - cls._CONSTRUCTOR_PARAMS.keys():
            setattr(op, k, None)

        # Set all the template_field to None that were not present in Serialized JSON
        for field in op.template_fields:
            if not hasattr(op, field):
                setattr(op, field, None)

        # Used to determine if an Operator is inherited from DummyOperator
        setattr(op, "_is_dummy", bool(encoded_op.get("_is_dummy", False)))

        return op

    @classmethod
    def _is_excluded(cls, var: Any, attrname: str, op: BaseOperator):
        if var is not None and op.has_dag() and attrname.endswith("_date"):
            # If this date is the same as the matching field in the dag, then
            # don't store it again at the task level.
            dag_date = getattr(op.dag, attrname, None)
            if var is dag_date or var == dag_date:
                return True
        return super()._is_excluded(var, attrname, op)

    @classmethod
    def _deserialize_deps(cls, deps: List[str]) -> Set["BaseTIDep"]:
        instances = set()
        for qualname in set(deps):
            if not qualname.startswith("airflow.ti_deps.deps."):
                log.error("Dep class %r not registered", qualname)
                continue

            try:
                instances.add(import_string(qualname)())
            except ImportError:
                log.warning("Error importing dep %r", qualname, exc_info=True)
        return instances

    @classmethod
    def _deserialize_operator_extra_links(cls, encoded_op_links: list) -> Dict[str, BaseOperatorLink]:
        """
        Deserialize Operator Links if the Classes  are registered in Airflow Plugins.
        Error is raised if the OperatorLink is not found in Plugins too.

        :param encoded_op_links: Serialized Operator Link
        :return: De-Serialized Operator Link
        """
        from airflow import plugins_manager

        plugins_manager.initialize_extra_operators_links_plugins()

        if plugins_manager.registered_operator_link_classes is None:
            raise AirflowException("Can't load plugins")
        op_predefined_extra_links = {}

        for _operator_links_source in encoded_op_links:
            # Get the key, value pair as Tuple where key is OperatorLink ClassName
            # and value is the dictionary containing the arguments passed to the OperatorLink
            #
            # Example of a single iteration:
            #
            #   _operator_links_source =
            #   {
            #       'airflow.providers.google.cloud.operators.bigquery.BigQueryConsoleIndexableLink': {
            #           'index': 0
            #       }
            #   },
            #
            #   list(_operator_links_source.items()) =
            #   [
            #       (
            #           'airflow.providers.google.cloud.operators.bigquery.BigQueryConsoleIndexableLink',
            #           {'index': 0}
            #       )
            #   ]
            #
            #   list(_operator_links_source.items())[0] =
            #   (
            #       'airflow.providers.google.cloud.operators.bigquery.BigQueryConsoleIndexableLink',
            #       {
            #           'index': 0
            #       }
            #   )

            _operator_link_class_path, data = list(_operator_links_source.items())[0]
            if _operator_link_class_path in get_operator_extra_links():
                single_op_link_class = import_string(_operator_link_class_path)
            elif _operator_link_class_path in plugins_manager.registered_operator_link_classes:
                single_op_link_class = plugins_manager.registered_operator_link_classes[
                    _operator_link_class_path
                ]
            else:
                log.error("Operator Link class %r not registered", _operator_link_class_path)
                return {}

            op_predefined_extra_link: BaseOperatorLink = cattr.structure(data, single_op_link_class)

            op_predefined_extra_links.update({op_predefined_extra_link.name: op_predefined_extra_link})

        return op_predefined_extra_links

    @classmethod
    def _serialize_operator_extra_links(cls, operator_extra_links: Iterable[BaseOperatorLink]):
        """
        Serialize Operator Links. Store the import path of the OperatorLink and the arguments
        passed to it. Example
        ``[{'airflow.providers.google.cloud.operators.bigquery.BigQueryConsoleLink': {}}]``

        :param operator_extra_links: Operator Link
        :return: Serialized Operator Link
        """
        serialize_operator_extra_links = []
        for operator_extra_link in operator_extra_links:
            op_link_arguments = cattr.unstructure(operator_extra_link)
            if not isinstance(op_link_arguments, dict):
                op_link_arguments = {}
            serialize_operator_extra_links.append(
                {
                    "{}.{}".format(
                        operator_extra_link.__class__.__module__, operator_extra_link.__class__.__name__
                    ): op_link_arguments
                }
            )

        return serialize_operator_extra_links


class SerializedDAG(DAG, BaseSerialization):
    """
    A JSON serializable representation of DAG.

    A stringified DAG can only be used in the scope of scheduler and webserver, because fields
    that are not serializable, such as functions and customer defined classes, are casted to
    strings.

    Compared with SimpleDAG: SerializedDAG contains all information for webserver.
    Compared with DagPickle: DagPickle contains all information for worker, but some DAGs are
    not pickle-able. SerializedDAG works for all DAGs.
    """

    _decorated_fields = {'schedule_interval', 'default_args', '_access_control'}

    @staticmethod
    def __get_constructor_defaults():  # pylint: disable=no-method-argument
        param_to_attr = {
            'concurrency': '_concurrency',
            'description': '_description',
            'default_view': '_default_view',
            'access_control': '_access_control',
        }
        return {
            param_to_attr.get(k, k): v.default
            for k, v in signature(DAG.__init__).parameters.items()
            if v.default is not v.empty
        }

    _CONSTRUCTOR_PARAMS = __get_constructor_defaults.__func__()  # type: ignore
    del __get_constructor_defaults

    _json_schema = load_dag_schema()

    @classmethod
    def serialize_dag(cls, dag: DAG) -> dict:
        """Serializes a DAG into a JSON object."""
        try:
            serialize_dag = cls.serialize_to_json(dag, cls._decorated_fields)

            serialize_dag["tasks"] = [cls._serialize(task) for _, task in dag.task_dict.items()]
            serialize_dag['_task_group'] = SerializedTaskGroup.serialize_task_group(dag.task_group)

            # has_on_*_callback are only stored if the value is True, as the default is False
            if dag.has_on_success_callback:
                serialize_dag['has_on_success_callback'] = True
            if dag.has_on_failure_callback:
                serialize_dag['has_on_failure_callback'] = True
            return serialize_dag
        except SerializationError:
            raise
        except Exception:
            raise SerializationError(f'Failed to serialize dag {dag.dag_id!r}')

    @classmethod
    def deserialize_dag(cls, encoded_dag: Dict[str, Any]) -> 'SerializedDAG':
        """Deserializes a DAG from a JSON object."""
        dag = SerializedDAG(dag_id=encoded_dag['_dag_id'])

        for k, v in encoded_dag.items():
            if k == "_downstream_task_ids":
                v = set(v)
            elif k == "tasks":
                # pylint: disable=protected-access
                SerializedBaseOperator._load_operator_extra_links = cls._load_operator_extra_links
                # pylint: enable=protected-access
                v = {task["task_id"]: SerializedBaseOperator.deserialize_operator(task) for task in v}
                k = "task_dict"
            elif k == "timezone":
                v = cls._deserialize_timezone(v)
            elif k in {"dagrun_timeout"}:
                v = cls._deserialize_timedelta(v)
            elif k.endswith("_date"):
                v = cls._deserialize_datetime(v)
            elif k in cls._decorated_fields:
                v = cls._deserialize(v)
            # else use v as it is

            setattr(dag, k, v)

        # Set _task_group
        # pylint: disable=protected-access
        if "_task_group" in encoded_dag:
            dag._task_group = SerializedTaskGroup.deserialize_task_group(  # type: ignore
                encoded_dag["_task_group"], None, dag.task_dict
            )
        else:
            # This must be old data that had no task_group. Create a root TaskGroup and add
            # all tasks to it.
            dag._task_group = TaskGroup.create_root(dag)
            for task in dag.tasks:
                dag.task_group.add(task)
        # pylint: enable=protected-access

        # Set has_on_*_callbacks to True if they exist in Serialized blob as False is the default
        if "has_on_success_callback" in encoded_dag:
            dag.has_on_success_callback = True
        if "has_on_failure_callback" in encoded_dag:
            dag.has_on_failure_callback = True

        keys_to_set_none = dag.get_serialized_fields() - encoded_dag.keys() - cls._CONSTRUCTOR_PARAMS.keys()
        for k in keys_to_set_none:
            setattr(dag, k, None)

        setattr(dag, 'full_filepath', dag.fileloc)
        for task in dag.task_dict.values():
            task.dag = dag
            serializable_task: BaseOperator = task

            for date_attr in ["start_date", "end_date"]:
                if getattr(serializable_task, date_attr) is None:
                    setattr(serializable_task, date_attr, getattr(dag, date_attr))

            if serializable_task.subdag is not None:
                setattr(serializable_task.subdag, 'parent_dag', dag)
                serializable_task.subdag.is_subdag = True

            for task_id in serializable_task.downstream_task_ids:
                # Bypass set_upstream etc here - it does more than we want
                # noqa: E501 # pylint: disable=protected-access
                dag.task_dict[task_id]._upstream_task_ids.add(serializable_task.task_id)

        return dag

    @classmethod
    def to_dict(cls, var: Any) -> dict:
        """Stringifies DAGs and operators contained by var and returns a dict of var."""
        json_dict = {"__version": cls.SERIALIZER_VERSION, "dag": cls.serialize_dag(var)}

        # Validate Serialized DAG with Json Schema. Raises Error if it mismatches
        cls.validate_schema(json_dict)
        return json_dict

    @classmethod
    def from_dict(cls, serialized_obj: dict) -> 'SerializedDAG':
        """Deserializes a python dict in to the DAG and operators it contains."""
        ver = serialized_obj.get('__version', '<not present>')
        if ver != cls.SERIALIZER_VERSION:
            raise ValueError(f"Unsure how to deserialize version {ver!r}")
        return cls.deserialize_dag(serialized_obj['dag'])


class SerializedTaskGroup(TaskGroup, BaseSerialization):
    """A JSON serializable representation of TaskGroup."""

    @classmethod
    def serialize_task_group(cls, task_group: TaskGroup) -> Optional[Union[Dict[str, Any]]]:
        """Serializes TaskGroup into a JSON object."""
        if not task_group:
            return None

        serialize_group = {
            "_group_id": task_group._group_id,  # pylint: disable=protected-access
            "prefix_group_id": task_group.prefix_group_id,
            "tooltip": task_group.tooltip,
            "ui_color": task_group.ui_color,
            "ui_fgcolor": task_group.ui_fgcolor,
            "children": {
                label: (DAT.OP, child.task_id)
                if isinstance(child, BaseOperator)
                else (DAT.TASK_GROUP, SerializedTaskGroup.serialize_task_group(child))
                for label, child in task_group.children.items()
            },
            "upstream_group_ids": cls._serialize(list(task_group.upstream_group_ids)),
            "downstream_group_ids": cls._serialize(list(task_group.downstream_group_ids)),
            "upstream_task_ids": cls._serialize(list(task_group.upstream_task_ids)),
            "downstream_task_ids": cls._serialize(list(task_group.downstream_task_ids)),
        }

        return serialize_group

    @classmethod
    def deserialize_task_group(
        cls,
        encoded_group: Dict[str, Any],
        parent_group: Optional[TaskGroup],
        task_dict: Dict[str, BaseOperator],
    ) -> Optional[TaskGroup]:
        """Deserializes a TaskGroup from a JSON object."""
        if not encoded_group:
            return None

        group_id = cls._deserialize(encoded_group["_group_id"])
        kwargs = {
            key: cls._deserialize(encoded_group[key])
            for key in ["prefix_group_id", "tooltip", "ui_color", "ui_fgcolor"]
        }
        group = SerializedTaskGroup(group_id=group_id, parent_group=parent_group, **kwargs)
        group.children = {
            label: task_dict[val]
            if _type == DAT.OP  # type: ignore
            else SerializedTaskGroup.deserialize_task_group(val, group, task_dict)
            for label, (_type, val) in encoded_group["children"].items()
        }
        group.upstream_group_ids = set(cls._deserialize(encoded_group["upstream_group_ids"]))
        group.downstream_group_ids = set(cls._deserialize(encoded_group["downstream_group_ids"]))
        group.upstream_task_ids = set(cls._deserialize(encoded_group["upstream_task_ids"]))
        group.downstream_task_ids = set(cls._deserialize(encoded_group["downstream_task_ids"]))
        return group
