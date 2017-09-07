#Statistics

To get statistics in Solr requests, besides the parameter to enable the [Mtas query component](search_component.html), the following parameter should be used.

| Parameter   |  Value | Obligatory  |
|-------------|--------|-------------|
| mtas.stats  | true   | yes         |

Using this parameter, it is possible to add statistics on [positions](search_component_stats_positions.html), [tokens](search_component_stats_tokens.html) and [spans](search_component_stats_spans.html) to the response on a request.

