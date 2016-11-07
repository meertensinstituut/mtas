$(document).ready(function() {	
	$("div.output").html("").hide();
	$("div.error").html("").hide();
});

function update(url, json, output, error, button) {
	output.html("").hide();
	error.html("<pre>waiting for response...</pre>").show();
	button.hide();
	$.ajax({
		'type' : 'POST',
		'url' : url,
		'contentType': 'application/json',
		'data' : json,
		'success' : function(data) {
			// create response
			try {
				var myObject = JSON.parse(data);
				error.hide();
				output.html(
								'<pre>'
										+ JSON.stringify(myObject,
												undefined, 2).replace(/&/g,
												'&amp;').replace(/</g,
												'&lt;').replace(/>/g,
												'&gt;').replace(/"/g,
												'&quot;') + '</pre>').show();				
			} catch (err) {
				error.html('<pre>'+data+'</pre>').show();
			}
			button.show();
		},
		'error' : function(jqXHR, textStatus, errorThrown) {
			error.html('<pre>'+
							jqXHR.status + ' : ' + jqXHR.statusText+ ' : ' +jqXHR.responseText+'</pre>').show();
			button.show();
		}
	});
}

