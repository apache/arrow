// Load the rmarkdown tabset script
$('head').append('<script src="https://cdn.jsdelivr.net/gh/rstudio/rmarkdown@47d837d3d9cd5e8e212b05767454f058db7d2789/inst/rmd/h/navigation-1.1/tabsets.js" integrity="sha256-Rs54TE1FCN1uLM4f7VQEMiRTl1Ia7TiQLkMruItwV+Q=" crossorigin="anonymous"></script>');

// Monkey patch the .html method to use the .text method
$(document).ready(function () {
  (function($){
    $.fn.html  = function(content){
      return this.text();
    };
})(jQuery);

window.buildTabsets("toc");
  });

  $(document).ready(function () {
    $('.tabset-dropdown > .nav-tabs > li').click(function () {
      $(this).parent().toggleClass('nav-tabs-open');
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
