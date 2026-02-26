#!/usr/bin/env python3
import os
import shutil
import sys
import subprocess

from setuptools import setup
from setuptools.command.install import install
from pathlib import Path

SPARK_HOME = os.getenv('SPARK_HOME')
TEMP_PATH = "deps"
VERSION_PATH = "VERSION"
JARS_TARGET = os.path.join(TEMP_PATH, "jars")
SCALA_SPARK_DIR = Path("../scala-spark-sdk")
UBER_JAR_NAME_PREFIX = "sagemaker-feature-store-spark-sdk"
UBER_JAR_NAME = f"{UBER_JAR_NAME_PREFIX}.jar"

in_spark_sdk = os.path.isfile(SCALA_SPARK_DIR / "build.sbt")
# read the contents of your README file
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


def read_version():
    return read(VERSION_PATH).strip()


# This is a post installation step. It will copy feature store spark uber jar to $SPARK_HOME/jars
class CustomInstall(install):
    def run(self):
        install.run(self)
        spark_home_dir = os.environ.get('SPARK_HOME', None)
        if spark_home_dir:
            uber_jar_target_dir = Path(spark_home_dir) / "jars"

            jars_in_deps = os.listdir(Path(os.getcwd()) / Path(JARS_TARGET))
            uber_jar_names = [jar for jar in jars_in_deps if jar.startswith(UBER_JAR_NAME_PREFIX)]
            for uber_jar_name in uber_jar_names:
                uber_jar_dir = Path(os.getcwd()) / Path(JARS_TARGET) / uber_jar_name
                print(f"Copying feature store uber jar {uber_jar_name} to {uber_jar_target_dir}")
                shutil.copy(uber_jar_dir, uber_jar_target_dir / uber_jar_name)

        else:
            print("Environment variable SPARK_HOME is not set, dependent jars are not installed to SPARK_HOME.")
        print("Installation finished.")


print("Starting the installation of SageMaker FeatureStore pyspark...")
if in_spark_sdk:
    shutil.copyfile(os.path.join("..", VERSION_PATH), VERSION_PATH)

    if not os.path.exists(JARS_TARGET):
        os.mkdir(JARS_TARGET)

    supported_spark_versions = ["3.2.4", "3.3.4", "3.4.3", "3.5.1"]
    for sv in supported_spark_versions:
        # use sbt to package the scala uber jar
        p = subprocess.Popen(["sbt", f'-DSPARK_VERSION={sv}', "assembly"],
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             cwd=SCALA_SPARK_DIR)
        p.communicate()

        # retrieve all jars under 'assembly-output'
        classpath = []
        assembly_output_dir = SCALA_SPARK_DIR / "assembly-output"
        assembly_output_files = os.listdir(assembly_output_dir)
        for output_file in assembly_output_files:
            file_path = assembly_output_dir / output_file
            if output_file.endswith(".jar") and os.path.exists(file_path):
                classpath.append(file_path)

        if len(classpath) == 0:
            print(f"Failed to retrieve the jar classpath. Can't package {sv}")
            exit(-1)
        
        # Ensure we get the latest assembled jar
        classpath.sort(key=os.path.getmtime)
        uber_jar_path = classpath[-1]

        sv_parts = sv.split(".")
        major_minor = f"{sv_parts[0]}.{sv_parts[1]}"
        target_jar_name = f"{UBER_JAR_NAME_PREFIX}-{major_minor}.jar"
        target_path = os.path.join(JARS_TARGET, target_jar_name)
        shutil.copy(uber_jar_path, target_path)

else:
    if not os.path.exists(JARS_TARGET):
        print("You need to be in the sagemaker-feature-store-spark root folder to package", file=sys.stderr)
        exit(-1)

setup(
    name="sagemaker_feature_store_pyspark_3.1",
    author="Amazon Web Services",

    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords="ML Amazon AWS AI FeatureStore SageMaker",

    version=read_version(),
    description="Amazon SageMaker FeatureStore PySpark Bindings",
    license="Apache License 2.0",
    zip_safe=False,

    packages=["feature_store_pyspark",
              "feature_store_pyspark.jars"],

    package_dir={
        "feature_store_pyspark": "src/feature_store_pyspark",
        "feature_store_pyspark.jars": "deps/jars"
    },
    include_package_data=True,

    scripts=["bin/feature-store-pyspark-dependency-jars"],

    package_data={
        "feature_store_pyspark.jars": ["*.jar"],
    },

    install_requires=[],

    cmdclass={
        'install': CustomInstall
    }
)
