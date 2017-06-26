from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import os
import random
import signal
import subprocess
import sys
import threading
import time
import unittest

import plasma
import pyarrow as pa

DEFAULT_PLASMA_STORE_MEMORY = 10 ** 9

USE_VALGRIND = False

def random_name():
  return str(random.randint(0, 99999999))

def random_object_id():
    return plasma.ObjectID(np.random.bytes(20))

def generate_metadata(length):
  metadata_buffer = bytearray(length)
  if length > 0:
    metadata_buffer[0] = random.randint(0, 255)
    metadata_buffer[-1] = random.randint(0, 255)
    for _ in range(100):
      metadata_buffer[random.randint(0, length - 1)] = random.randint(0, 255)
  return metadata_buffer

def assert_get_object_equal(unit_test, client1, client2, object_id,
                            memory_buffer=None, metadata=None):
  client1_buff = client1.get([object_id])[0]
  client2_buff = client2.get([object_id])[0]
  client1_metadata = client1.get_metadata([object_id])[0]
  client2_metadata = client2.get_metadata([object_id])[0]
  unit_test.assertEqual(len(client1_buff), len(client2_buff))
  unit_test.assertEqual(len(client1_metadata), len(client2_metadata))
  # Check that the buffers from the two clients are the same.
  unit_test.assertTrue(plasma.buffers_equal(client1_buff, client2_buff))
  # Check that the metadata buffers from the two clients are the same.
  unit_test.assertTrue(plasma.buffers_equal(client1_metadata,
                                            client2_metadata))
  # If a reference buffer was provided, check that it is the same as well.
  if memory_buffer is not None:
    unit_test.assertTrue(plasma.buffers_equal(memory_buffer, client1_buff))
  # If reference metadata was provided, check that it is the same as well.
  if metadata is not None:
    unit_test.assertTrue(plasma.buffers_equal(metadata, client1_metadata))

def start_plasma_store(plasma_store_memory=DEFAULT_PLASMA_STORE_MEMORY,
                       use_valgrind=False, use_profiler=False,
                       stdout_file=None, stderr_file=None):
  """Start a plasma store process.
  Args:
    use_valgrind (bool): True if the plasma store should be started inside of
      valgrind. If this is True, use_profiler must be False.
    use_profiler (bool): True if the plasma store should be started inside a
      profiler. If this is True, use_valgrind must be False.
    stdout_file: A file handle opened for writing to redirect stdout to. If no
      redirection should happen, then this should be None.
    stderr_file: A file handle opened for writing to redirect stderr to. If no
      redirection should happen, then this should be None.
  Return:
    A tuple of the name of the plasma store socket and the process ID of the
      plasma store process.
  """
  if use_valgrind and use_profiler:
    raise Exception("Cannot use valgrind and profiler at the same time.")
  plasma_store_executable = os.path.join(os.path.abspath(
      os.path.dirname(__file__)),
      "../../../build/debug/plasma_store")
  plasma_store_name = "/tmp/plasma_store{}".format(random_name())
  command = [plasma_store_executable,
             "-s", plasma_store_name,
             "-m", str(plasma_store_memory)]
  if use_valgrind:
    pid = subprocess.Popen(["valgrind",
                            "--track-origins=yes",
                            "--leak-check=full",
                            "--show-leak-kinds=all",
                            "--error-exitcode=1"] + command,
                           stdout=stdout_file, stderr=stderr_file)
    time.sleep(1.0)
  elif use_profiler:
    pid = subprocess.Popen(["valgrind", "--tool=callgrind"] + command,
                           stdout=stdout_file, stderr=stderr_file)
    time.sleep(1.0)
  else:
    pid = subprocess.Popen(command, stdout=stdout_file, stderr=stderr_file)
    time.sleep(0.1)
  return plasma_store_name, pid

class TestPlasmaClient(unittest.TestCase):

  def setUp(self):
    # Start Plasma store.
    plasma_store_name, self.p = start_plasma_store(
        use_valgrind=USE_VALGRIND)
    # Connect to Plasma.
    self.plasma_client = plasma.PlasmaClient()
    self.plasma_client.connect(plasma_store_name, "", 64)
    # For the eviction test
    self.plasma_client2 = plasma.PlasmaClient()
    self.plasma_client2.connect(plasma_store_name, "", 0)

  def tearDown(self):
    # Check that the Plasma store is still alive.
    self.assertEqual(self.p.poll(), None)
    # Kill the plasma store process.
    if USE_VALGRIND:
      self.p.send_signal(signal.SIGTERM)
      self.p.wait()
      if self.p.returncode != 0:
        os._exit(-1)
    else:
      self.p.kill()

  def test_create(self):
    # Create an object id string.
    object_id = random_object_id()
    # Create a new buffer and write to it.
    length = 50
    memory_buffer = np.frombuffer(self.plasma_client.create(object_id, length), dtype="uint8")
    for i in range(length):
      memory_buffer[i] = i % 256
    # Seal the object.
    self.plasma_client.seal(object_id)
    # Get the object.
    memory_buffer = np.frombuffer(self.plasma_client.get([object_id])[0], dtype="uint8")
    for i in range(length):
      self.assertEqual(memory_buffer[i], i % 256)

  def test_create_with_metadata(self):
    for length in range(1000):
      # Create an object id string.
      object_id = random_object_id()
      # Create a random metadata string.
      metadata = generate_metadata(length)
      # Create a new buffer and write to it.
      memory_buffer = np.frombuffer(self.plasma_client.create(object_id, length, metadata), dtype="uint8")
      for i in range(length):
        memory_buffer[i] = i % 256
      # Seal the object.
      self.plasma_client.seal(object_id)
      # Get the object.
      memory_buffer = np.frombuffer(self.plasma_client.get([object_id])[0], dtype="uint8")
      for i in range(length):
        self.assertEqual(memory_buffer[i], i % 256)
      # Get the metadata.
      metadata_buffer = np.frombuffer(self.plasma_client.get_metadata([object_id])[0], dtype="uint8")
      self.assertEqual(len(metadata), len(metadata_buffer))
      for i in range(len(metadata)):
        self.assertEqual(metadata[i], metadata_buffer[i])

  def test_create_existing(self):
    # This test is partially used to test the code path in which we create an
    # object with an ID that already exists
    length = 100
    for _ in range(1000):
      object_id = random_object_id()
      self.plasma_client.create(object_id, length, generate_metadata(length))
      try:
        self.plasma_client.create(object_id, length, generate_metadata(length))
      except pa.lib.ArrowException as e:
        pass
      else:
        self.assertTrue(False)

if __name__ == "__main__":
  if len(sys.argv) > 1:
    # Pop the argument so we don't mess with unittest's own argument parser.
    if sys.argv[-1] == "valgrind":
      arg = sys.argv.pop()
      USE_VALGRIND = True
      print("Using valgrind for tests")
  unittest.main(verbosity=2)
