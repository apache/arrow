(function() {
    // adapted from https://scikit-learn.org/versionwarning.js
    var latestStable = '10.0.0';
    var showWarning = (msg) => {
        $('.bd-header-announcement container-fluid').prepend(
            '<p style="' +
            [
                'padding: 1em',
                'font-size: 1.5em',
                'border: 1px solid red',
                'background: pink',
            ].join('; ') +
            '">' + msg + '</p>')
    };
    if (location.hostname == 'arrow.apache.org') {
        var versionPath = location.pathname.split('/')[2];
        if (versionPath == 'developers') {
            // developers section in the stable version
            showWarning('This is documentation for the stable version ' +
                        'of Apache Arrow. For latest contributing practices ' +
                        'switch to unstable development ' +
                        'release <a href="https://arrow.apache.org/docs/dev/developers/contributing.html">version ' +
                        latestStable + '</a>.')
        } else if (versionPath.match(/^\d/) || location.pathname.split('/')[3] == 'developers') {
            // older versions of developers section (with numbered version in the URL)
            showWarning('This is documentation for an old release of Apache Arrow ' +
                        '(version ' + versionPath + '). For latest development practices ' +
                        'switch to unstable development release ' +
                        '<a href="https://arrow.apache.org/docs/dev/developers/contributing.html">version ' +
                        latestStable + '</a>.')
        } else if (versionPath == 'dev') {
            // older versions (with numbered version in the URL)
            showWarning('This is documentation for an old release of ' +
                        'Apache Arrow (version ' + versionPath + '). Try the ' +
                        '<a href="https://arrow.apache.org/docs/index.html">latest stable ' +
                        'release</a> (version ' + latestStable + ') or ' +
                        '<a href="https://arrow.apache.org/docs/dev/index.html">development</a> ' +
                        '(unstable) version.')
        } else if (versionPath == 'dev') {
            showWarning('This is documentation for the unstable ' +
                        'development version of Apache Arrow.' + 
                        'The latest stable ' +
                        'release is <a href="https://arrow.apache.org/docs/index.html">version ' +
                        latestStable + '</a>.')
        }
    }
})()
