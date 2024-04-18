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


import argparse
import contextlib
import http.server
import multiprocessing
import os
import re
import time

from pathlib import Path
from io import BytesIO

from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions


class TemplateOverrider(http.server.SimpleHTTPRequestHandler):
    def log_request(self, code="-", size="-"):
        # don't log successful requests
        return

    def do_GET(self) -> bytes | None:
        if self.path.endswith(PYARROW_WHEEL_PATH.name):
            self.send_response(200)
            self.send_header("Content-type", "application/x-zip")
            self.end_headers()
            self.copyfile(PYARROW_WHEEL_PATH.open(mode="rb"), self.wfile)
        if self.path.endswith("/test.html"):
            body = b"""
                <!doctype html>
                <html>
                <head>
                    <script>
                        window.pyworker = new Worker("worker.js");
                    </script>
                </head>
                <body></body>
                </html>
                """
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.send_header("Content-length", len(body))
            self.end_headers()
            self.copyfile(BytesIO(body), self.wfile)
        elif self.path.endswith("/worker.js"):
            body = b"""
                importScripts("./pyodide.js");
                onmessage = async function (e) {
                    if(!self.pyodide){
                        self.pyodide = await loadPyodide()

                        var cur_line;
                        function do_print(arg){
                            lines=arg.split("\\n");
                            cur_line+=lines[0]
                            if(lines.length>1){
                                for(let c=1;c<lines.length;c++){
                                    console.log(cur_line);
                                    cur_line=lines[c]
                                }
                            }
                        }
                        self.pyodide.setStdout(do_print);
                    }

                    const data = e.data;
                    await self.pyodide.loadPackagesFromImports(data.python);
                    let results = await self.pyodide.runPythonAsync(data.python);
                    self.postMessage({results});
                }
                """
            self.send_response(200)
            self.send_header("Content-type", "application/javascript")
            self.send_header("Content-length", len(body))
            self.end_headers()
            self.copyfile(BytesIO(body), self.wfile)

        else:
            return super().do_GET()

    def end_headers(self):
        # Enable Cross-Origin Resource Sharing (CORS)
        self.send_header("Access-Control-Allow-Origin", "*")
        super().end_headers()


def run_server_thread(dist_dir, q):
    global _SERVER_ADDRESS
    os.chdir(dist_dir)
    server = http.server.HTTPServer(("", 0), TemplateOverrider)
    q.put(server.server_address)
    print(f"Starting server for {dist_dir} at: {server.server_address}")
    server.serve_forever()


@contextlib.contextmanager
def launch_server(dist_dir):
    q = multiprocessing.Queue()
    p = multiprocessing.Process(target=run_server_thread, args=[dist_dir, q])
    p.start()
    address = q.get(timeout=50)
    yield address
    p.terminate()


def exec_python(driver, code, wait_for_terminate=True):
    if wait_for_terminate:
        driver.execute_async_script(
            f"""
            let callback=arguments[arguments.length-1];
            python = `{code}`;
            window.pyworker.onmessage=callback;
            window.pyworker.postMessage({{python}})
            """
        )
    else:
        driver.execute_script(
            f"""
            let python = `{code}`;
            window.pyworker.onmessage=undefined;
            window.pyworker.postMessage({{python}});
            """
        )


def _load_pyarrow_in_runner(driver, wheel_name):
    exec_python(
        driver,
        f"""import sys
import micropip
if "pyarrow" not in sys.modules:
    try:
        import tzdata
    except:
        await micropip.install("tzdata")
    await micropip.install("hypothesis")
    import pyodide_js as pjs
    await pjs.loadPackage("{wheel_name}")
    await pjs.loadPackage("numpy")
    await pjs.loadPackage("pandas")
    import pytest
    import pandas # import pandas after pyarrow package load for pandas/pyarrow
                    #functions to work
import pyarrow
    """,
        wait_for_terminate=True,
    )


parser = argparse.ArgumentParser()
parser.add_argument(
    "-d",
    "--dist-dir",
    type=str,
    help="Pyodide distribution directory",
    default="./pyodide",
)
parser.add_argument("wheel", type=str, help="Wheel to run tests from")
parser.add_argument(
    "-t", "--test-submodule", help="Submodule that tests live in", default="test"
)
parser.add_argument(
    "-r",
    "--runtime",
    type=str,
    choices=["firefox", "chrome", "node", "safari"],
    help="Runtime to run tests in ",
    default="chrome",
)
args = parser.parse_args()

PYARROW_WHEEL_PATH = Path(args.wheel).resolve()

dist_dir = Path(os.getcwd(), args.dist_dir).resolve()
print(f"dist dir={dist_dir}")
with launch_server(dist_dir) as (hostname, port):
    options = ChromeOptions()
    options.set_capability("goog:loggingPrefs", {"browser": "ALL"})
    driver = webdriver.Chrome(options=options)
    driver.get(f"http://{hostname}:{port}/test.html")
    driver.set_script_timeout(100)

    _load_pyarrow_in_runner(driver, Path(args.wheel).name)
    driver.get_log("browser")  # clear logs for webdriver
    exec_python(
        driver,
        """
import pyarrow,pathlib
pyarrow_dir = pathlib.Path(pyarrow.__file__).parent / "tests"
pytest.main([pyarrow_dir,'-v'])
""",
        wait_for_terminate=False,
    )
    while True:
        # poll for console.log messages from our webworker
        # which are the output of pytest
        for x in driver.get_log("browser"):
            if "source" in x and x["source"] == "worker":
                # messages are of the form: URL id <MESSAGE>
                message_text = re.match(r"http://\S+ \d* (.*)", x["message"])
                if message_text:
                    print(message_text.group(1))
        time.sleep(1)
