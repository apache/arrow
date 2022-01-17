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

(function () {
  // Load the rmarkdown tabset script
  var script = document.createElement("script");
  script.type = "text/javascript";
  script.async = true;
  script.src =
    "https://cdn.jsdelivr.net/gh/rstudio/rmarkdown@47d837d3d9cd5e8e212b05767454f058db7d2789/inst/rmd/h/navigation-1.1/tabsets.js";
  script.integrity = "sha256-Rs54TE1FCN1uLM4f7VQEMiRTl1Ia7TiQLkMruItwV+Q=";
  script.crossOrigin = "anonymous";

  // Run the processing as the onload callback
  script.onload = () => {
    // Monkey patch the .html method to use the .text method
    $(document).ready(function () {
      (function ($) {
        $.fn.html = function (content) {
          return this.text();
        };
      })(jQuery);

      window.buildTabsets("toc");
    });

    $(document).ready(function () {
      $(".tabset-dropdown > .nav-tabs > li").click(function () {
        $(this).parent().toggleClass("nav-tabs-open");
      });
    });

    $(document).ready(function () {
      /**
       * The tabset creation above sometimes relies on empty headers to stop the
       * tabbing. Though they shouldn't be included in the TOC in the first place,
       * this will remove empty headers from the TOC after it's created.
       */

      // find all the empty <a> elements and remove them (and their parents)
      var empty_a = $("#toc").find("a").filter(":empty");
      empty_a.parent().remove();

      // now find any empty <ul>s and remove them too
      var empty_ul = $("#toc").find("ul").filter(":empty");
      empty_ul.remove();
    });

  $(document).ready(function () {
      /**
       * This replaces the package version number in the docs with a
       * dropdown where you can select the version of the docs to view.
       */

      $pathStart = function(){
    	  return window.location.origin + "/docs/";
      }

      $pathEnd  = function(){
      	var current_path = window.location.pathname;
      	return current_path.match("(?<=\/r).*");
      }

      // Load the versions JSON and construct the select items
      // Delete below line before merging
      // You can test with https://raw.githubusercontent.com/thisisnic/arrow/ARROW-14338_pkgdown_versioning/r/pkgdown/assets/versions.json
      //$.getJSON("./versions.json", function( data ) {
      $.getJSON("https://raw.githubusercontent.com/thisisnic/arrow/ec8c60d97eb489f0c19297e8d9b3f48e44db5afb/r/pkgdown/assets/versions.json", function( data ) {

        var items = [];
        // get the current page's version number:
				var displayed_version = $('.version').text();
				$.each( data, function( key, val ) {

					var selected_string = (
						val.name.match("[0-9.]*")[0] === displayed_version ?
						"selected" :
						""
					 );

          var item_path = $pathStart() + val.version + "r" + $pathEnd();

					items.push("<option value='" + item_path +  "'" + selected_string +	">" + val.name + "</option>");
				});

				// Replace the version button with a selector with the doc versions
				$("span.version").replaceWith(
					'<select name="version-selector" class = "navbar-default" '+
					'onchange="location = this.value;"> ' + items.join("") + '</select> '
				);
			});
		});

	};

	document.head.appendChild(script);
})();
