#
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

"""Serialized DAG table in database."""

import hashlib
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import sqlalchemy_jsonfield
from sqlalchemy import BigInteger, Column, Index, String, and_
from sqlalchemy.orm import Session, backref, foreign, relationship
from sqlalchemy.sql.expression import literal

from airflow.models.base import ID_LEN, Base
from airflow.models.dag import DAG, DagModel
from airflow.models.dagcode import DagCode
from airflow.models.dagrun import DagRun
from airflow.serialization.serialized_objects import DagDependency, SerializedDAG
from airflow.settings import MIN_SERIALIZED_DAG_UPDATE_INTERVAL, json
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.utils.sqlalchemy import UtcDateTime

log = logging.getLogger(__name__)


class SerializedDagModel(Base):
    """A table for serialized DAGs.

    serialized_dag table is a snapshot of DAG files synchronized by scheduler.
    This feature is controlled by:

    * ``[core] min_serialized_dag_update_interval = 30`` (s):
      serialized DAGs are updated in DB when a file gets processed by scheduler,
      to reduce DB write rate, there is a minimal interval of updating serialized DAGs.
    * ``[scheduler] dag_dir_list_interval = 300`` (s):
      interval of deleting serialized DAGs in DB when the files are deleted, suggest
      to use a smaller interval such as 60

    It is used by webserver to load dags
    because reading from database is lightweight compared to importing from files,
    it solves the webserver scalability issue.
    """

    __tablename__ = 'serialized_dag'

    dag_id = Column(String(ID_LEN), primary_key=True)
    fileloc = Column(String(2000), nullable=False)
    # The max length of fileloc exceeds the limit of indexing.
    fileloc_hash = Column(BigInteger, nullable=False)
    data = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=False)
    last_updated = Column(UtcDateTime, nullable=False)
    dag_hash = Column(String(32), nullable=False)

    __table_args__ = (Index('idx_fileloc_hash', fileloc_hash, unique=False),)

    dag_runs = relationship(
        DagRun,
        primaryjoin=dag_id == foreign(DagRun.dag_id),
        backref=backref('serialized_dag', uselist=False, innerjoin=True),
    )

    dag_model = relationship(
        DagModel,
        primaryjoin=dag_id == DagModel.dag_id,  # type: ignore
        foreign_keys=dag_id,
        uselist=False,
        innerjoin=True,
        backref=backref('serialized_dag', uselist=False, innerjoin=True),
    )

    load_op_links = True

    def __init__(self, dag: DAG):
        self.dag_id = dag.dag_id
        self.fileloc = dag.full_filepath
        self.fileloc_hash = DagCode.dag_fileloc_hash(self.fileloc)
        self.data = SerializedDAG.to_dict(dag)
        self.last_updated = timezone.utcnow()
        self.dag_hash = hashlib.md5(json.dumps(self.data, sort_keys=True).encode("utf-8")).hexdigest()

    def __repr__(self):
        return f"<SerializedDag: {self.dag_id}>"

    @classmethod
    @provide_session
    def write_dag(cls, dag: DAG, min_update_interval: Optional[int] = None, session: Session = None) -> bool:
        """Serializes a DAG and writes it into database.
        If the record already exists, it checks if the Serialized DAG changed or not. If it is
        changed, it updates the record, ignores otherwise.

        :param dag: a DAG to be written into database
        :param min_update_interval: minimal interval in seconds to update serialized DAG
        :param session: ORM Session

        :returns: Boolean indicating if the DAG was written to the DB
        """
        # Checks if (Current Time - Time when the DAG was written to DB) < min_update_interval
        # If Yes, does nothing
        # If No or the DAG does not exists, updates / writes Serialized DAG to DB
        if min_update_interval is not None:
            if (
                session.query(literal(True))
                .filter(
                    and_(
                        cls.dag_id == dag.dag_id,
                        (timezone.utcnow() - timedelta(seconds=min_update_interval)) < cls.last_updated,
                    )
                )
                .first()
                is not None
            ):
                return

        log.debug("Checking if DAG (%s) changed", dag.dag_id)
        new_serialized_dag = cls(dag)
        serialized_dag_hash_from_db = session.query(cls.dag_hash).filter(cls.dag_id == dag.dag_id).scalar()

        if serialized_dag_hash_from_db == new_serialized_dag.dag_hash:
            log.debug("Serialized DAG (%s) is unchanged. Skipping writing to DB", dag.dag_id)
            return False

        log.debug("Writing Serialized DAG: %s to the DB", dag.dag_id)
        session.merge(new_serialized_dag)
        log.debug("DAG: %s written to the DB", dag.dag_id)
        return True

    @classmethod
    @provide_session
    def read_all_dags(cls, session: Session = None) -> Dict[str, 'SerializedDAG']:
        """Reads all DAGs in serialized_dag table.

        :param session: ORM Session
        :returns: a dict of DAGs read from database
        """
        serialized_dags = session.query(cls)

        dags = {}
        for row in serialized_dags:
            log.debug("Deserializing DAG: %s", row.dag_id)
            dag = row.dag

            # Sanity check.
            if dag.dag_id == row.dag_id:
                dags[row.dag_id] = dag
            else:
                log.warning(
                    "dag_id Mismatch in DB: Row with dag_id '%s' has Serialised DAG with '%s' dag_id",
                    row.dag_id,
                    dag.dag_id,
                )
        return dags

    @property
    def dag(self):
        """The DAG deserialized from the ``data`` column"""
        SerializedDAG._load_operator_extra_links = self.load_op_links  # pylint: disable=protected-access

        if isinstance(self.data, dict):
            dag = SerializedDAG.from_dict(self.data)  # type: Any
        else:
            dag = SerializedDAG.from_json(self.data)  # noqa
        return dag

    @classmethod
    @provide_session
    def remove_dag(cls, dag_id: str, session: Session = None):
        """Deletes a DAG with given dag_id.

        :param dag_id: dag_id to be deleted
        :param session: ORM Session
        """
        # pylint: disable=no-member
        session.execute(cls.__table__.delete().where(cls.dag_id == dag_id))

    @classmethod
    @provide_session
    def remove_deleted_dags(cls, alive_dag_filelocs: List[str], session=None):
        """Deletes DAGs not included in alive_dag_filelocs.

        :param alive_dag_filelocs: file paths of alive DAGs
        :param session: ORM Session
        """
        alive_fileloc_hashes = [DagCode.dag_fileloc_hash(fileloc) for fileloc in alive_dag_filelocs]

        log.debug(
            "Deleting Serialized DAGs (for which DAG files are deleted) from %s table ", cls.__tablename__
        )

        # pylint: disable=no-member
        session.execute(
            cls.__table__.delete().where(
                and_(cls.fileloc_hash.notin_(alive_fileloc_hashes), cls.fileloc.notin_(alive_dag_filelocs))
            )
        )

    @classmethod
    @provide_session
    def has_dag(cls, dag_id: str, session: Session = None) -> bool:
        """Checks a DAG exist in serialized_dag table.

        :param dag_id: the DAG to check
        :param session: ORM Session
        """
        return session.query(literal(True)).filter(cls.dag_id == dag_id).first() is not None

    @classmethod
    @provide_session
    def get(cls, dag_id: str, session: Session = None) -> Optional['SerializedDagModel']:
        """
        Get the SerializedDAG for the given dag ID.
        It will cope with being passed the ID of a subdag by looking up the
        root dag_id from the DAG table.

        :param dag_id: the DAG to fetch
        :param session: ORM Session
        """
        row = session.query(cls).filter(cls.dag_id == dag_id).one_or_none()
        if row:
            return row

        # If we didn't find a matching DAG id then ask the DAG table to find
        # out the root dag
        root_dag_id = session.query(DagModel.root_dag_id).filter(DagModel.dag_id == dag_id).scalar()

        return session.query(cls).filter(cls.dag_id == root_dag_id).one_or_none()

    @staticmethod
    @provide_session
    def bulk_sync_to_db(dags: List[DAG], session: Session = None):
        """
        Saves DAGs as Serialized DAG objects in the database. Each
        DAG is saved in a separate database query.

        :param dags: the DAG objects to save to the DB
        :type dags: List[airflow.models.dag.DAG]
        :param session: ORM Session
        :type session: Session
        :return: None
        """
        for dag in dags:
            if not dag.is_subdag:
                SerializedDagModel.write_dag(
                    dag, min_update_interval=MIN_SERIALIZED_DAG_UPDATE_INTERVAL, session=session
                )

    @classmethod
    @provide_session
    def get_last_updated_datetime(cls, dag_id: str, session: Session = None) -> datetime:
        """
        Get the date when the Serialized DAG associated to DAG was last updated
        in serialized_dag table

        :param dag_id: DAG ID
        :type dag_id: str
        :param session: ORM Session
        :type session: Session
        """
        return session.query(cls.last_updated).filter(cls.dag_id == dag_id).scalar()

    @classmethod
    @provide_session
    def get_max_last_updated_datetime(cls, session: Session = None) -> datetime:
        """
        Get the maximum date when any DAG was last updated in serialized_dag table

        :param session: ORM Session
        :type session: Session
        """
        return session.query(func.max(cls.last_updated)).scalar()

    @classmethod
    @provide_session
    def get_latest_version_hash(cls, dag_id: str, session: Session = None) -> str:
        """
        Get the latest DAG version for a given DAG ID.

        :param dag_id: DAG ID
        :type dag_id: str
        :param session: ORM Session
        :type session: Session
        :return: DAG Hash
        :rtype: str
        """
        return session.query(cls.dag_hash).filter(cls.dag_id == dag_id).scalar()

    @classmethod
    @provide_session
    def get_dag_dependencies(cls, session: Session = None) -> Dict[str, List['DagDependency']]:
        """
        Get the dependencies between DAGs

        :param session: ORM Session
        :type session: Session
        """
        dependencies = {}

        if session.bind.dialect.name in ["sqlite", "mysql"]:
            for row in session.query(cls.dag_id, func.json_extract(cls.data, "$.dag.dag_dependencies")).all():
                dependencies[row[0]] = [DagDependency(**d) for d in json.loads(row[1])]

        else:
            for row in session.query(
                cls.dag_id, func.json_extract_path(cls.data, "dag", "dag_dependencies")
            ).all():
                dependencies[row[0]] = [DagDependency(**d) for d in row[1]]

        return dependencies
