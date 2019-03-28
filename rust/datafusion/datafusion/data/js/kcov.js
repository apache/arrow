window.onload = function () {

	// http://stackoverflow.com/questions/8853396/logical-operator-in-a-handlebars-js-if-conditional
	Handlebars.registerHelper('ifCond', function (v1, operator, v2, options) {
		switch (operator) {
		case '==':
			return (v1 == v2) ? options.fn(this) : options.inverse(this);
		case '===':
			return (v1 === v2) ? options.fn(this) : options.inverse(this);
		case '<':
			return (v1 < v2) ? options.fn(this) : options.inverse(this);
		case '<=':
			return (v1 <= v2) ? options.fn(this) : options.inverse(this);
		case '>':
			return (v1 > v2) ? options.fn(this) : options.inverse(this);
		case '>=':
			return (v1 >= v2) ? options.fn(this) : options.inverse(this);
		case '&&':
			return (v1 && v2) ? options.fn(this) : options.inverse(this);
		case '||':
			return (v1 || v2) ? options.fn(this) : options.inverse(this);
		default:
			return options.inverse(this);
		}
	});

	// http://stackoverflow.com/questions/17095813/handlebars-if-and-numeric-zeroes
	Handlebars.registerHelper('exists', function(variable, options) {
	    if (typeof variable !== 'undefined') {
	        return options.fn(this);
	    } else {
	        return options.inverse(this);
	    }
	});

	var filesElement = document.getElementById("files-template")
	if (filesElement) {
		var source   = filesElement.innerHTML;
		var template = Handlebars.compile(source);

		document.getElementById('files-placeholder').innerHTML = template(data);
	}
	var linesElement = document.getElementById("lines-template")
	if (linesElement) {
		var source   = linesElement.innerHTML;
		var template = Handlebars.compile(source);
		document.getElementById('lines-placeholder').innerHTML = template(data);
	}

	elem = document.getElementById('header-percent-covered')

	elem.className = toCoverPercentString(header.covered, header.instrumented);
	elem.innerHTML = ((header.covered / header.instrumented) * 100).toFixed(1) + "%";	    

	document.getElementById('header-command').innerHTML = header.command;
	document.getElementById('window-title').innerHTML = "Coverage report - " + header.command;
	document.getElementById('header-date').innerHTML = header.date;
	document.getElementById('header-covered').innerHTML = header.covered
	document.getElementById('header-instrumented').innerHTML = header.instrumented

	$("#index-table").tablesorter({
		theme : 'blue',
		sortList : [[1,0]],
		cssInfoBlock : "tablesorter-no-sort",
		widgets: ["saveSort"],
	});
}

function toCoverPercentString (covered, instrumented) {
	perc = (covered / instrumented) * 100;

	if (perc <= percent_low)
		return "coverPerLeftLo";
	else if (perc >= percent_high)
		return "coverPerLeftHi";

	return "coverPerLeftMed";
}
