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

import textwrap
from buildbot.plugins import util
from buildbot.process.results import FAILURE, SUCCESS
from buildbot.test.fake import fakedb
from ursabot.utils import ensure_deferred
from ursabot.tests.test_formatters import TestFormatterBase

from ..formatters import BenchmarkCommentFormatter, CrossbowCommentFormatter


class TestBenchmarkCommentFormatter(TestFormatterBase):

    def setupFormatter(self):
        return BenchmarkCommentFormatter()

    def setupDb(self, current, previous):
        super().setupDb(current, previous)

        log1 = self.load_fixture('archery-benchmark-diff.jsonl')
        log2 = self.load_fixture('archery-benchmark-diff-empty-lines.jsonl')

        self.db.insertTestData([
            fakedb.Step(id=50, buildid=21, number=0, name='compile'),
            fakedb.Step(id=51, buildid=21, number=1, name='benchmark',
                        results=current),
            fakedb.Step(id=52, buildid=20, number=0, name='compile'),
            fakedb.Step(id=53, buildid=20, number=1, name='benchmark',
                        results=current),
            fakedb.Log(id=60, stepid=51, name='result', slug='result',
                       type='t', num_lines=4),
            fakedb.Log(id=61, stepid=53, name='result', slug='result',
                       type='t', num_lines=6),
            fakedb.LogChunk(logid=60, first_line=0, last_line=4, compressed=0,
                            content=log1),
            fakedb.LogChunk(logid=61, first_line=0, last_line=6, compressed=0,
                            content=log2)
        ])

    @ensure_deferred
    async def test_failure(self):
        status = 'failed.'
        expected = f"""
        [Builder1 (#{self.BUILD_ID})]({self.BUILD_URL}) builder {status}

        Revision: {self.REVISION}
        """
        content = await self.render(previous=SUCCESS, current=FAILURE)
        assert content == textwrap.dedent(expected).strip()

    @ensure_deferred
    async def test_success(self):
        status = 'has been succeeded.'
        expected = f"""
        [Builder1 (#{self.BUILD_ID})]({self.BUILD_URL}) builder {status}

        Revision: {self.REVISION}

        ```diff
          ============================  ===========  ===========  ===========
          benchmark                        baseline    contender       change
          ============================  ===========  ===========  ===========
          RegressionSumKernel/32768/50  1.92412e+10  1.92114e+10  -0.00155085
        - RegressionSumKernel/32768/1   2.48232e+10  2.47718e+10   0.00206818
          RegressionSumKernel/32768/10  2.19027e+10  2.19757e+10   0.00333234
        - RegressionSumKernel/32768/0   2.7685e+10   2.78212e+10  -0.00491813
          ============================  ===========  ===========  ===========
        ```
        """
        content = await self.render(previous=SUCCESS, current=SUCCESS,
                                    buildsetid=99)
        assert content == textwrap.dedent(expected).strip()

    @ensure_deferred
    async def test_empty_jsonlines(self):
        BUILD_URL = 'http://localhost:8080/#builders/80/builds/0'
        BUILD_ID = 20
        expected = f"""
        [Builder1 (#{BUILD_ID})]({BUILD_URL}) builder has been succeeded.

        Revision: {self.REVISION}

        ```diff
          ============================  ===========  ===========  ==========
          benchmark                        baseline    contender      change
          ============================  ===========  ===========  ==========
          RegressionSumKernel/32768/10  1.32654e+10  1.33275e+10  0.00467565
          RegressionSumKernel/32768/1   1.51819e+10  1.522e+10    0.00251084
          RegressionSumKernel/32768/50  1.14718e+10  1.15116e+10  0.00346736
          RegressionSumKernel/32768/0   1.8317e+10   1.85027e+10  0.010141
          ============================  ===========  ===========  ==========
        ```
        """
        content = await self.render(previous=SUCCESS, current=SUCCESS,
                                    buildsetid=98)
        assert content == textwrap.dedent(expected).strip()


class TestCrossbowCommentFormatter(TestFormatterBase):

    def setupFormatter(self):
        return CrossbowCommentFormatter(
            crossbow_repo=util.Property('crossbow_repo')
        )

    def setupDb(self, current, previous):
        super().setupDb(current, previous)

        job = self.load_fixture('crossbow-job.yaml')

        self.db.insertTestData([
            fakedb.Step(id=50, buildid=21, number=0, name='compile'),
            fakedb.Step(id=51, buildid=21, number=1, name='benchmark',
                        results=current),
            fakedb.Log(id=60, stepid=51, name='result', slug='result',
                       type='t', num_lines=len(job)),
            fakedb.LogChunk(logid=60, first_line=0, last_line=len(job),
                            compressed=0, content=job)
        ])
        for _id in (20, 21):
            self.db.insertTestData([
                fakedb.BuildProperty(buildid=_id, name='crossbow_repo',
                                     value='ursa-labs/crossbow')
            ])

    @ensure_deferred
    async def test_success(self):
        expected_msg = self.load_fixture('crossbow-success-message.md').format(
            repo='ursa-labs/crossbow',
            branch='ursabot-1',
            status='has been succeeded.',
            revision=self.REVISION,
            build_id=self.BUILD_ID,
            build_url=self.BUILD_URL
        )
        content = await self.render(previous=SUCCESS, current=SUCCESS,
                                    buildsetid=99)
        assert content == textwrap.dedent(expected_msg).strip()
