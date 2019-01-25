// the content of this file is embedded at the bottom of the catalog HTML page

var app = new Vue({
    el: '#dataset_view',
    data: {
        dsinfo: {},
        coinfo: {},
        ds_by_path: {},
        alerts: []
    },
});

// request vars as object attributes
var request_vars = getRequests();

if ('id' in request_vars) {
    console.log("ERR: You haven't thought of me, yet!");
} else {
    // request by path within superdataset
    // go with superdataset itself if no path is given
    if (!('p' in request_vars)) {
        var path = "."
    } else {
        var path = request_vars['p']
    }
    // get the path inventory
    axios.get("by_path.json", {
            params: {
                json: 'yes'
            }
        })
        .then(function (response) {
            // cache result
            app.$data.ds_by_path = response.data;
            // obtain object info for target dataset, Vue does the rest
            assign_metadata_obj(response.data[path], 'dsinfo');
        })
        .catch(function (error) {
            push_msg("cannot load dataset inventory: [" + error +"]", 'error');
        });
}
