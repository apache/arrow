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

function check_page_exists_and_redirect(event) {

    /**
       * When a user uses the version dropdown in the docs, check if the page
       * they are currently browsing exists in that version of the docs.
       * If yes, take them there; if no, take them to the main docs page.
       */

    const path_to_try = event.target.value;

    const base_path = path_to_try.match("(.*\/r\/)?")[0];
    let tryUrl = path_to_try;
    $.ajax({
        type: 'HEAD',
        url: tryUrl,
        success: function() {
            location.href = tryUrl;
        }
    }).fail(function() {
        location.href = base_path;
    });
    return false;
}

/**
 * We need to do this so that the version dropdown doesn't used cached HTML
 * on Chrome on MacOS (see ARROW-15895)
 */
$(window).bind("pageshow", function(event) {
  if (event.originalEvent.persisted) {
    window.location.reload()
  }
});

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

         // Get the start of the path which includes the version number or "dev"
         // where applicable and add the "/docs/" suffix
        $pathStart = function(){
    	  return window.location.origin + "/docs/";
        }


        // Get the end of the path after the version number or "dev" if present
        $pathEnd  = function(){
      	  var current_path = window.location.pathname;
      	  // Can't do this via a positive look-behind or we lose Safari compatibility
      	  return current_path.match("\/r.*")[0].substr(2);
        }

        // Load JSON file mapping between docs version and R package version
        $.getJSON("https://arrow.apache.org/docs/r/versions.json", function( data ) {
          // get the current page's version number:
    		  var displayed_version = $('.version').text();
    		  // Create a dropdown selector and add the appropriate attributes
          const sel = document.createElement("select");
          sel.name = "version-selector";
          sel.id = "version-selector";
          sel.classList.add("navbar-default");
          // When the selected value is changed, take the user to the version
          // of the page they are browsing in the selected version
          sel.onchange = check_page_exists_and_redirect;

          // For each of the items in the JSON object (name/version pairs)
    		  $.each( data, function( key, val ) {
    		    // Add a new option to the dropdown selector
            const opt = document.createElement("option");
            // Set the path based on the 'version' field
            opt.value = $pathStart() + val.version + "r" + $pathEnd();
            // Set the currently selected item based on the major and minor version numbers
            opt.selected = val.name.match("[0-9.]*")[0] === displayed_version;
            // Set the displayed text based on the 'name' field
            opt.text = val.name;
            // Add to the selector
            sel.append(opt);
  		    });

          // Replace the HTML "version" component with the new selector
          $("span.version").replaceWith(sel);
        });
    });

  };

  document.head.appendChild(script);
})();
