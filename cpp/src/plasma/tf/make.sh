TF_INC=$(python -c 'import tensorflow as tf; print(tf.sysconfig.get_include())')

g++ -std=c++11 -g -shared plasma_op.cc -o plasma_op.so `pkg-config --cflags --libs plasma` -undefined dynamic_lookup -fPIC -I $TF_INC -O2
