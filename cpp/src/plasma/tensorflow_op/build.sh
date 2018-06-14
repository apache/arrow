set -ex

TF_CFLAGS=$(python -c 'import tensorflow as tf; print(" ".join(tf.sysconfig.get_compile_flags()))')
TF_LFLAGS=$(python -c 'import tensorflow as tf; print(" ".join(tf.sysconfig.get_link_flags()))')

if [ "$(uname)" == "Darwin" ]; then
    TF_CFLAGS="-undefined dynamic_lookup ${TF_CFLAGS}"
fi

NDEBUG="-DNDEBUG"

g++ -std=c++11 -g -shared tf_plasma_op.cc -o tf_plasma_op.so \
    ${NDEBUG} \
    `pkg-config --cflags --libs plasma arrow` \
    -fPIC \
    ${TF_CFLAGS[@]} \
    ${TF_LFLAGS[@]} \
    -O2

cp tf_plasma_op.so ../../../../python/pyarrow
