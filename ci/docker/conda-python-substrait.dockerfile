ARG repo
ARG arch
ARG python=3.9

FROM ${repo}:${arch}-conda-python-${python}

COPY ci/conda_env_python.txt \
     ci/conda_env_sphinx.txt \
     /arrow/ci/
RUN mamba install -q -y \
        --file arrow/ci/conda_env_python.txt \
        --file arrow/ci/conda_env_sphinx.txt \
        $([ "$python" == "3.7" ] && echo "pickle5") \
        python=${python} \
        nomkl && \
    mamba clean --all


ARG substrait=latest
COPY ci/scripts/install_substrait_consumer.sh /arrow/ci/scripts/
# RUN /arrow/ci/scripts/install_substrait_consumer.sh ${substrait}


ENV ARROW_BUILD_TESTS=ON \
    ARROW_COMPUTE=ON \
    ARROW_CSV=ON \
    ARROW_DATASET=ON \
    ARROW_FILESYSTEM=ON \
    ARROW_JSON=ON \
    ARROW_SUBSTRAIT=ON \
    ARROW_USE_GLOG=OFF
