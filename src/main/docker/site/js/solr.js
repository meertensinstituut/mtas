$(document).ready(
		function() {
			$("div.output").html("").hide();
			$("div.error").html("").hide();

			$("div.solr").each(function() {
				var text = $(this).find("div.post textarea").first().text();
				$(this).find("input.reset").first().data("original", text);
			});
			$("div.solr input.reset").click(
					function() {
						var parent = $(this).closest("div.solr");
						parent.find("div.output").html("").hide();
						parent.find("div.error").html("").hide();
						parent.find("div.post textarea").first().val(
								$(this).data("original"));
					});
			$("div.solr input.post").click(
					function() {
						var parent = $(this).closest("div.solr");
						var url = parent.data('url');
						var type = parent.data('type');
						$("div.solr div.output").html("").hide();
						$("div.solr div.error").html("").hide();
						update(url, type, parent.find("div.post textarea").first(), parent.find("div.output")
								.first(), parent.find("div.error").first(),
								parent.find("input.create").first());
					});
			jQuery.each(jQuery('textarea[data-autoresize]'), function() {
				var offset = this.offsetHeight - this.clientHeight;

				var resizeTextarea = function(el) {
					jQuery(el).css('height', 'auto').css('height',
							el.scrollHeight + offset);
				};
				resizeTextarea(this);
				jQuery(this).on('keyup input', function() {
					resizeTextarea(this);
				}).removeAttr('data-autoresize');
			});
		});

function update(url, type, post, output, error, button) {
	output.html("").hide();
	if (type == "json") {
		error.html("<pre>waiting for response...</pre>").show();
		button.hide();
		$
				.ajax({
					'type' : 'POST',
					'url' : url,
					'contentType' : 'application/json',
					'data' : post.val(),
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
													'&quot;') + '</pre>')
									.show();
						} catch (err) {
							error.html('<pre>' + data + '</pre>').show();
						}
						button.show();
					},
					'error' : function(jqXHR, textStatus, errorThrown) {
						error.html(
								'<pre>' + jqXHR.status + ' : '
										+ jqXHR.statusText + ' : '
										+ jqXHR.responseText + '</pre>').show();
						button.show();
					}
				});
	} else if (type == "post") {
		error.html("<pre>waiting for response...</pre>").show();
		button.hide();
		$
				.ajax({
					'type' : 'POST',
					'url' : url,
					'data' : post.val(),
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
													'&quot;') + '</pre>')
									.show();
						} catch (err) {
							error.html('<pre>' + data + '</pre>').show();
						}
						button.show();
					},
					'error' : function(jqXHR, textStatus, errorThrown) {
						error.html(
								'<pre>' + jqXHR.status + ' : '
										+ jqXHR.statusText + ' : '
										+ jqXHR.responseText + '</pre>').show();
						button.show();
					}
				});
	} else {
		error.html("<pre>unexpected request type " + type + "...</pre>").show();
		button.hide();
	}
}
