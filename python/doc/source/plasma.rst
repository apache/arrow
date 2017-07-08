.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

.. currentmodule:: pyarrow
.. _io:

The Plasma In-Memory Object Store
=================================

.. contents:: Contents
    :depth: 3

Installation on Ubuntu
----------------------
The following install instructions have been tested for Ubuntu 16.04.


First, install Anaconda in your terminal as follows. This will download 
the Anaconda Linux installer and run it. Be sure to invoke the installer 
with the ``bash`` command, whether or not you are using the Bash shell.

.. code-block:: bash

    wget https://repo.continuum.io/archive/Anaconda3-4.4.0-Linux-x86_64.sh
    bash Anaconda3-4.4.0-Linux-x86_64.sh

.. note::

	As an alternative to the wget command above, you can also download the 
	Anaconda installer script through your web browser at their 
	`Download Webpage here <https://www.continuum.io/downloads#linux>`_.


Accept the Anaconda license agreement and follow the prompt. Allow the 
installer to prepend the Anaconda location to your PATH. 

Then, either close and reopen your terminal window, or run the following 
command, so that the new PATH takes effect:

.. code-block:: bash

    source ~/.bashrc

Anaconda should now be installed. For more information on installing 
Anaconda, see their `documentation here <https://docs.continuum.io/anaconda/install/linux>`_.


Next, update your system and install the following dependency packages 
as below:

.. code-block:: bash

    sudo apt-get update
    sudo apt-get install -y cmake build-essential autoconf curl libtool libboost-all-dev 
    sudo apt-get install -y unzip libjemalloc-dev pkg-config
    sudo ldconfig


Now, we need to install arrow. These instructions will install everything 
to your home directory. First download the arrow package from github:

.. code-block:: bash

    cd ~
    git clone https://github.com/apache/arrow
    
Next, create a build directory as follows:

.. code-block:: bash

    cd arrow/cpp
    git checkout plasma-cython
    mkdir build
    cd build

You should now be in the ~/arrow/cpp/build directory. Run cmake and
make to build Arrow.

.. code-block:: bash

    cmake -DARROW_PYTHON=on -DARROW_PLASMA=on -DARROW_BUILD_TESTS=off ..
    make
    sudo make install

.. warning::

	Running the ``cmake`` command above may give an ``ImportError`` 
	concerning numpy. If that is the case, see `ImportError when Running Cmake`_.


After installing arrow, you need to install pyarrow as follows:

.. code-block:: bash

	cd ~/arrow/python
	python setup.py install

Once you've installed pyarrow, you should verify that you are able to 
import it when running python in the terminal:

.. code-block:: shell

	ubuntu:~/arrow/cpp/src/plasma$ python
	Python 3.6.1 |Anaconda custom (64-bit)| (default, May 11 2017, 13:09:58) 
	[GCC 4.4.7 20120313 (Red Hat 4.4.7-1)] on linux
	Type "help", "copyright", "credits" or "license" for more information.
	>>> import pyarrow
	>>>

If you encounter an ImportError when running the above, see `ImportError After Installing Pyarrow`_.

Finally, you can install Plasma.

.. code-block:: bash

	cd ~/arrow/cpp/src/plasma
	python setup.py install

Similar to pyarrow, you can verify that Plasma has been installed by
trying to import it when running python. Make sure to try this from
outside of the ~/arrow/cpp/src/plasma directory, otherwise you may 
encounter the following error:

.. code-block:: shell

	ubuntu:~/arrow/cpp/src/plasma$ python
	Python 3.6.1 |Anaconda custom (64-bit)| (default, May 11 2017, 13:09:58) 
	[GCC 4.4.7 20120313 (Red Hat 4.4.7-1)] on linux
	Type "help", "copyright", "credits" or "license" for more information.
	>>> import plasma
	Traceback (most recent call last):
	  File "<stdin>", line 1, in <module>
	  File "/home/ubuntu/arrow/cpp/src/plasma/plasma/__init__.py", line 18, in <module>
	    from .plasma import *
	ModuleNotFoundError: No module named 'plasma.plasma'


Installation on Mac OS X (TODO)
-------------------------------
The following install instructions have been tested for Mac OS X 10.9 
Mavericks.


First, install Anaconda as follows. Download the Graphical MacOS
Installer for your version of Python at the `Anaconda Download Webpage here <https://www.continuum.io/downloads#macos>`_.

Double-click on the ``.pkg`` file, accept the license agreement, and 
follow the step-by-step wizard to install Anaconda. Anaconda will be 
installed for the current user's use only, and will require about 1.44 
GB of space.

To verify that Anaconda has been installed, click on the Launchpad and
select Anaconda Navigator. It should open if you have successfully
installed Anaconda. For more information on installing Anaconda, see 
their `documentation here <https://docs.continuum.io/anaconda/install/mac-os.html>`_.

The next step is to install the following dependency packages as below:

.. code-block:: bash

    brew update
    brew install cmake autoconf libtool pkg-config jemalloc

Plasma also requires the build-essential, curl, unzip, libboost-all-dev, 
and libjemalloc-dev packages. MacOS should already come with curl, unzip, 
and the compilation tools found in build-essential. Ldconfig is not supported
on Mac.

Now, install arrow as follows. Open your terminal window and download the 
arrow package from github with the following commands:

.. code-block:: bash

    cd ~
    git clone https://github.com/apache/arrow
    
Create a directory for the arrow build:

.. code-block:: bash

    cd arrow/cpp
    git checkout plasma-cython
    mkdir build
    cd build

You should now be in the ~/arrow/cpp/build directory. Run cmake and
make to build Arrow.

.. code-block:: bash

    cmake -DARROW_PYTHON=on -DARROW_PLASMA=on -DARROW_BUILD_TESTS=off ..
    make
    sudo make install

TODO:

* Install Pyarrow
* Verify Pyarrow
* Install Plasma



Troubleshooting Installation Issues
-----------------------------------

ImportError when Running Cmake
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

While installing arrow, if you run into the following error when running 
the ``cmake`` command, there may be an issue with finding numpy.

.. code-block:: shell

	    NumPy import failure:

  	Traceback (most recent call last):

    	File "<string>", line 1, in <module>

  	ImportError: No module named numpy

First, verify that numpy has been installed alongside anaconda. Running
``conda list`` outputs all the packages that have been installed with
anaconda:

.. code-block:: shell

	ubuntu:~/arrow/cpp/build$ conda list
	numpy                     1.12.1                   py36_0 

If something similar to the above numpy line is not listed in the 
output, numpy has not yet been installed.

If numpy has not been installed, try running the following command:

.. code-block:: bash

	conda install numpy

If numpy is still not installed, try reinstalling anaconda.

Second, verify that you are running the python version that comes with
anaconda. ``which`` should point to the python in the newly-installed
Anaconda package:

.. code-block:: shell

	ubuntu:~/arrow/cpp/build$ which python
	/home/ubuntu/anaconda3/bin/python

If this issue comes up, most likely the anaconda library has not yet
been properly prepended to your PATH and the new PATH reloaded. 

If your machine already has other python versions installed, the Anaconda 
python path should precede any other python version path. You can find 
the paths to all python versions installed on your machine by running 
``whereis python`` in the terminal:

.. code-block:: shell

	ubuntu:~/arrow/cpp/build$ whereis python
	python: /usr/bin/python3.5m /usr/bin/python2.7 /usr/bin/python /usr/bin/python2.7-config /usr/bin/python3.5 /usr/lib/python2.7 /usr/lib/python3.5 /etc/python2.7 /etc/python /etc/python3.5 /usr/local/lib/python2.7 /usr/local/lib/python3.5 /usr/include/python2.7 /usr/share/python /home/ubuntu/anaconda3/bin/python3.6m-config /home/ubuntu/anaconda3/bin/python3.6m /home/ubuntu/anaconda3/bin/python3.6 /home/ubuntu/anaconda3/bin/python3.6-config /home/ubuntu/anaconda3/bin/python /usr/share/man/man1/python.1.gz

Anaconda usually modifies your ``~/.bashrc`` file in its installation. 
You may need to manually add the following line or similar to the bottom 
of your ``~/.bashrc`` file, then reload your terminal window:

.. code-block:: bash

	# added by Anaconda3 4.4.0 installer
	export PATH="/home/ubuntu/anaconda3/bin:$PATH"

You can also create a persistent ``python`` shell alias to point to your 
Anaconda python version by adding to following to the bottom of your 
``~/.bashrc`` file:

.. code-block:: bash

	alias python=/home/ubuntu/anaconda3/bin/python

At this point, if you no longer have any issues with your anaconda 
installation or with your python version, you should be able to run Python 
in the terminal and import numpy with no errors:

.. code-block:: shell

	ubuntu:~/arrow/cpp/build$ python
	Python 3.6.1 |Anaconda 4.4.0 (64-bit)| (default, May 11 2017, 13:09:58) 
	[GCC 4.4.7 20120313 (Red Hat 4.4.7-1)] on linux
	Type "help", "copyright", "credits" or "license" for more information.
	>>> import numpy
	>>>

Finally, if you are confident that numpy has been installed and that you are
using Anaconda's version of python, cmake may be looking for python and
finding the wrong version (not Anaconda's version of python). Run the following
command instead (setting the ``FILEPATH`` to the path of your Anaconda python 
version) to force ``cmake`` to use the correct python version:

.. code-block:: bash

	cmake -DPYTHON_EXECUTABLE:FILEPATH=/home/ubuntu/anaconda3/bin/python -DARROW_PYTHON=on -DARROW_PLASMA=on -DARROW_BUILD_TESTS=off ..

You may now proceed with the rest of the arrow installation.


ImportError After Installing Pyarrow
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You may encounter the following error output when trying to ``import pyarrow`` 
inside Python:

.. code-block:: shell

	>>> import pyarrow
	Traceback (most recent call last):
	  File "<stdin>", line 1, in <module>
	  File "/home/ubuntu/anaconda3/lib/python3.6/site-packages/pyarrow-0.1.1.dev625+ge08c220-py3.6-linux-x86_64.egg/pyarrow/__init__.py", line 28, in <module>
	    from pyarrow.lib import cpu_count, set_cpu_count
	ImportError: libarrow.so.0: cannot open shared object file: No such file or directory

If this is the case, after you have built Arrow, try running the following line
again in the terminal to remove this ImportError:

.. code-block:: bash
	
	sudo ldconfig

You may also encounter the following error output when trying to ``import pyarrow``
inside Python:

.. code-block:: shell

	>>> import pyarrow
	Traceback (most recent call last):
	  File "<stdin>", line 1, in <module>
	  File "/home/ubuntu/anaconda3/lib/python3.6/site-packages/pyarrow-0.1.1.dev625+ge08c220-py3.6-linux-x86_64.egg/pyarrow/__init__.py", line 28, in <module>
	    from pyarrow.lib import cpu_count, set_cpu_count
	ImportError: /home/ubuntu/anaconda3/bin/../lib/libstdc++.so.6: version `GLIBCXX_3.4.21' not found (required by /home/ubuntu/anaconda3/lib/python3.6/site-packages/pyarrow-0.1.1.dev625+ge08c220-py3.6-linux-x86_64.egg/pyarrow/lib.cpython-36m-x86_64-linux-gnu.so)

If this is the case, run the following command to remove this ImportError:

.. code-block:: bash
	
	conda install -y libgcc


The Plasma API
--------------

Creating a Plasma client
^^^^^^^^^^^^^^^^^^^^^^^^

First locate your plasma directory. This can be printed out by 
importing plasma in python and running the command ``print(plasma.__path__)``. 
If running python from the terminal, be sure to run this command outside of the ~/arrow/cpp/src/plasma directory, or you may encounter an error. 

For example, to find your plasma directory, you can run the following one-liner 
from the terminal like follows:

.. code-block:: shell

	ubuntu:~$ python -c "import plasma; print(plasma.__path__)"
	['/home/ubuntu/anaconda3/lib/python3.6/site-packages/plasma-0.0.1-py3.6-linux-x86_64.egg/plasma']

From inside the plasma directory, you can start the plasma store in the 
foreground by issuing a terminal command similar to the following:

.. code-block:: bash

	./plasma_store -m 1000000000 -s /tmp/plasma

This command must be issued inside the plasma directory to work. The -m flag 
specifies the size of the store in bytes, and the -s flag specifies the socket
that the store will listen at. Thus, the above command sets the Plasma store 
to use up to 1 GB of memory, and sets the socket to ``/tmp/plasma``.

Leave the current terminal window open as long as Plasma store should keep 
running. Error messages, such as disconnecting clients, may occasionally be outputted.
To stop running the plasma store, you can press ``CTRL-C`` in the terminal.

Finally, from within python, the same socket given to ``./plasma_store`` 
should then be passed into the Plasma client as shown below:

.. code-block:: python

	import plasma
	client = plasma.PlasmaClient()
	client.connect("/tmp/plasma", "", 0)

If the following error occurs from running the above Python code, that
means that either the socket given is incorrect, or the ``./plasma_store`` is 
not currently running. Make sure that you are still running the ``./plasma_store`` 
process in your plasma directory.

.. code-block:: shell

	>>> client.connect("/tmp/plasma", "", 0)
	Connection to socket failed for pathname /tmp/plasma
	Could not connect to socket /tmp/plasma


Object IDs 
^^^^^^^^^^

Each object in the Plasma store should be associated with a unique id. The 
Object ID then serves as a key for any client to fetch that object from 
the Plasma store. You can form an ObjectID object from a byte string of 
length 20.

.. code-block:: shell

	# Create ObjectID of 20 bytes, each being the byte (b) encoding of the letter "a"
	>>> id = plasma.ObjectID(20 * b"a")  

	# "a" is encoded as 61
	>>> id
	ObjectID(6161616161616161616161616161616161616161)

Creating an Object
^^^^^^^^^^^^^^^^^^

Objects are created in Plasma in two stages. First, they are *created*, which 
allocates a buffer for the object. At this point, the client can write to the 
buffer and construct the object within the allocated buffer. 

.. code-block:: python

	# Create an object.
	object_id = plasma.ObjectID(20 * b"a")  # Note that this is an ObjectID object, not a string
	object_size = 1000
	buffer = memoryview(client.create(object_id, object_size))

	# Write to the buffer.
	for i in range(1000):
	    buffer[i] = i % 128

When the client is done, the client *seals* the buffer, making the object 
immutable, and making it available to other Plasma clients.

.. code-block:: python

	# Seal the object. This makes the object immutable and available to other clients.
	client.seal(object_id)


Getting an Object 
^^^^^^^^^^^^^^^^^

After an object has been sealed, any client who knows the object ID can get 
the object.

.. code-block:: python

	# Get the object from a different client. This blocks until the object has been sealed.
	object_id = plasma.ObjectID(20 * b"a")  
	[buffer] = client.get([object_id])  # Note that you pass in as an ObjectID object, not a string


If the object has not been sealed yet, then the call to client.get will block 
until the object has been sealed by the client constructing the object.

Storing Arrow Objects and Pandas DataFrames in Plasma (TODO)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can copy the examples from test_store_arrow_objects as well as 
test_store_pandas_dataframe in arrow/cpp/src/plasma/test/test.py; maybe explain 
a little bit what is going on (should become clear if you look into the pyarrow 
documentation a bit, let me know if it is not). Not that the 
test_store_pandas_dataframe test doesn't work at the moment, it probably is a 
bug in arrow; we are working on it and it will be fixed before the documentation 
goes online.

