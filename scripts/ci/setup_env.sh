#!/usr/bin/env bash
MINIKDC_VERSION=2.7.1

HADOOP_DISTRO=${HADOOP_DISTRO:-"hdp"}

ONLY_DOWNLOAD=${ONLY_DOWNLOAD:-false}
ONLY_EXTRACT=${ONLY_EXTRACT:-false}

while test $# -gt 0; do
    case "$1" in
        -h|--help)
            echo "Setup environment for airflow tests"
            echo " "
            echo "options:"
            echo -e "\t-h, --help            show brief help"
            echo -e "\t-o, --only-download   just download hadoop tar(s)"
            echo -e "\t-e, --only-extract    just extract hadoop tar(s)"
            echo -e "\t-d, --distro          select distro (hdp|cdh)"
            exit 0
            ;;
        -o|--only-download)
            shift
            ONLY_DOWNLOAD=true
            ;;
        -e|--only-extract)
            shift
            ONLY_EXTRACT=true
            ;;
        -d|--distro)
            shift
            if test $# -gt 0; then
                HADOOP_DISTRO=$1
            else
                echo "No Hadoop distro specified - abort" >&2
                exit 1
            fi
            shift
            ;;
        *)
            echo "Unknown options: $1" >&2
            exit 1
            ;;
    esac
done

HADOOP_HOME=/tmp/hadoop-${HADOOP_DISTRO}

if $ONLY_DOWNLOAD && $ONLY_EXTRACT; then
    echo "Both only-download and only-extract specified - abort" >&2
    exit 1
fi

mkdir -p $HADOOP_HOME

# Setup MiniKDC environment
mkdir -p $HADOOP_HOME/minikdc

URL=http://search.maven.org/remotecontent?filepath=org/apache/ivy/ivy/2.3.0/ivy-2.3.0.jar
echo "Downloading ivy"
curl -o ${HADOOP_HOME}/minikdc/ivy.jar -L ${URL}

if [ $? != 0 ]; then
    echo "Failed to download ivy"
    exit 1
fi

echo "Getting minikdc dependencies"
java -jar ${HADOOP_HOME}/minikdc/ivy.jar -dependency org.apache.hadoop hadoop-minikdc ${MINIKDC_VERSION} -retrieve "${HADDOP_HOME}/minikdc/lib/[artifact]-[revision](-[classifier]).[ext]"

if [ $? != 0 ]; then
    echo "Failed to download dependencies for minikdc"
    exit 1
fi

if [ $HADOOP_DISTRO = "cdh" ]; then
    URL="http://archive.cloudera.com/cdh5/cdh/5/hadoop-latest.tar.gz"
elif [ $HADOOP_DISTRO = "hdp" ]; then
    URL="http://public-repo-1.hortonworks.com/HDP/centos6/2.x/updates/2.0.6.0/tars/hadoop-2.2.0.2.0.6.0-76.tar.gz"
else
    echo "No/bad HADOOP_DISTRO='${HADOOP_DISTRO}' specified" >&2
    exit 1
fi

if ! $ONLY_EXTRACT; then
    echo "Downloading Hadoop from $URL to ${HADOOP_HOME}/hadoop.tar.gz"
    curl -z ${HADOOP_HOME}/hadoop.tar.gz -o ${HADOOP_HOME}/hadoop.tar.gz -L $URL

    if [ $? != 0 ]; then
        echo "Failed to download Hadoop from $URL - abort" >&2
        exit 1
    fi
fi

if $ONLY_DOWNLOAD; then
    exit 0
fi

echo "Extracting ${HADOOP_HOME}/hadoop.tar.gz into $HADOOP_HOME"
tar zxf ${HADOOP_HOME}/hadoop.tar.gz --strip-components 1 -C $HADOOP_HOME

if [ $? != 0 ]; then
    echo "Failed to extract Hadoop from ${HADOOP_HOME}/hadoop.tar.gz to ${HADOOP_HOME} - abort" >&2
    exit 1
fi


