// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

(function() {
    // adapted 2022-11 from https://mne.tools/versionwarning.js
    if (location.hostname == 'arrow.apache.org') {
        $.getJSON("https://arrow.apache.org/docs/_static/versions.json", function(data){
            var latestStable = data[1].name.replace(" (stable)","");
            // HTML tags
            var pre = '<div class="container-fluid alert-danger devbar"><div class="row no-gutters"><div class="col-12 text-center">';
            var post = '</div></div></div>';
            var anchor = 'class="btn btn-danger font-weight-bold ml-3 my-3 align-baseline"';
            // Switch button message
            var switch_dev = `Switch to unstable development release version`;
            var switch_stable = `latest stable release (version ${latestStable})`;
            // Path of the page
            var location_array = location.pathname.split('/');
            var versionPath = location_array[2];
            var majorVersionNumber = Number(versionPath.match(/^\d+/))
            var subPath = location_array[3];
            var filePath = location_array.slice(3).join('/');
            // Links to stable or dev versions
            var uri_dev = `https://arrow.apache.org/docs/dev/${filePath}`;
            var uri_stable = `https://arrow.apache.org/docs/${filePath}`;

            if (versionPath == 'developers') {
                // developers section in the stable version
                filePath = location_array.slice(2).join('/');
                uri_dev = `https://arrow.apache.org/docs/dev/${filePath}`;
                $.ajax({
                    type: 'HEAD',
                    url: `${uri_dev}`,
                    error: function() {
                        filePath = '';
                        uri_dev = `https://arrow.apache.org/docs/dev/${filePath}`;
                    },
                    complete: function() {
                        var showWarning = `${pre}This is documentation for the stable version ` +
                                        `of Apache Arrow (version ${latestStable}). For latest development practices: ` +
                                        `<a ${anchor} href=${uri_dev}>${switch_dev}</a>${post}`
                        $('.container-fluid').prepend(`${showWarning}`)
                    }
                });
            } else if (majorVersionNumber < 4) {
                // old versions 1.0, 2.0 or 3.0
                $.ajax({
                    type: 'HEAD',
                    url: `${uri_stable}`,
                    error: function() {
                        filePath = '';
                        uri_stable = `https://arrow.apache.org/docs/${filePath}`;
                    },
                    complete: function() {
                        $.ajax({
                            type: 'HEAD',
                            url: `${uri_dev}`,
                            error: function() {
                                filePath = '';
                                uri_dev = `https://arrow.apache.org/docs/dev/${filePath}`;
                            },
                            complete: function() {
                                pre = '<p style="padding: 1em;font-size: 1em;border: 1px solid red;background: pink;">';
                                post = '</p>';
                                anchor = 'class="btn btn-danger" style="font-weight: bold; vertical-align: baseline;' +
                                        'margin: 0.5rem; border-style: solid; border-color: white;"';
                                var showWarning = `${pre}This is documentation for an old release of ` +
                                                `Apache Arrow (version ${versionPath}). Try the` +
                                                `<a ${anchor} href=${uri_stable}>${switch_stable}</a> or` +
                                                `<a ${anchor} href=${uri_dev}>development (unstable) version. </a>${post}`
                                $('.document').prepend(`${showWarning}`)
                            }
                        });
                    }
                });
            } else if (majorVersionNumber && subPath == 'developers') {
                // older versions of developers section (with numbered version in the URL)
                $.ajax({
                    type: 'HEAD',
                    url: `${uri_dev}`,
                    error: function() {
                        filePath = '';
                        uri_dev = `https://arrow.apache.org/docs/dev/${filePath}`;
                    },
                    complete: function() {
                        var showWarning = `${pre}This is documentation for an old release of Apache Arrow ` +
                                        `(version ${versionPath}). For latest development practices: ` +
                                        `<a ${anchor} href=${uri_dev}>${switch_dev} </a>${post}`
                        $('.container-fluid').prepend(`${showWarning}`)
                    }
                });
            } else if (majorVersionNumber) {
                // older versions (with numbered version in the URL)
                $.ajax({
                    type: 'HEAD',
                    url: `${uri_stable}`,
                    error: function() {
                        filePath = '';
                        uri_stable = `https://arrow.apache.org/docs/${filePath}`;
                    },
                    complete: function() {
                        $.ajax({
                            type: 'HEAD',
                            url: `${uri_dev}`,
                            error: function() {
                                filePath = '';
                                uri_dev = `https://arrow.apache.org/docs/dev/${filePath}`;
                            },
                            complete: function() {
                                var showWarning = `${pre}This is documentation for an old release of ` +
                                                `Apache Arrow (version ${versionPath}). Try the` +
                                                `<a ${anchor} href=${uri_stable}>${switch_stable}</a> or` +
                                                `<a ${anchor} href=${uri_dev}>development (unstable) version. </a>${post}`
                                $('.container-fluid').prepend(`${showWarning}`)
                            }
                        });
                    }
                });
            }
        });
    }
})()
