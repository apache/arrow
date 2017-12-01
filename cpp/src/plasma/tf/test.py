import numpy as np
import pyarrow as pa
import pyarrow.plasma as plasma
import tensorflow as tf

import time

zero_out_module = tf.load_op_library('./plasma_op.so')

client = plasma.connect("/tmp/plasma", "", 64)

data = np.random.randn(10000, 4000).astype("float32")
tensor = pa.Tensor.from_numpy(data)

data_id = client.put(tensor)

# plasma.ObjectID(np.random.bytes(20))
# data_size = pa.get_tensor_size(tensor)
# buf = client.create(object_id, data_size)
# stream = pa.FixedSizeBufferWriter(buf)
# pa.write_tensor(tensor, stream)
# client.seal(object_id)

sess = tf.Session()
object_id = tf.placeholder(tf.string)
load_op = zero_out_module.plasma_data([object_id], socket="/tmp/plasma")
a = time.time()
print("XXX", sess.run(load_op, feed_dict={object_id: data_id.binary()}))
b = time.time() - a
print("b1", b)
print("XXX", sess.run(load_op, feed_dict={object_id: data_id.binary()}))

placeholder = tf.placeholder(tf.float32, shape=(10000, 4000))

# variable = tf.Variable(placeholder, trainable=False, initializer=tf.random_uniform_initializer(-1.0, 1.0))

# sess.run(tf.global_variables_initializer())
a = time.time()
d = sess.run(placeholder, feed_dict={placeholder: data})
b = time.time() - a
print("b2", b)


print("ZZZ", d)

print("YYY", data)
