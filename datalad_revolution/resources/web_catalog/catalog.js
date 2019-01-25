// the content of this file is embedded into the 

// helper to represent URL arguments as an object
function getRequests() {
    var s1 = location.search.substring(1, location.search.length).split('&'),
        r = {}, s2, i;
    for (i = 0; i < s1.length; i += 1) {
        s2 = s1[i].split('=');
        r[decodeURIComponent(s2[0]).toLowerCase()] = decodeURIComponent(s2[1]);
    }
    return r;
};

function assign_metadata_obj(path, target) {
    axios.get("objs/" + path, {
        params: {
            json: 'yes'
        }
    })
    .then(function (response) {
        app.$data[target] = response.data;
        if (target == 'dsinfo') {
            // advertise metadata in page
            document.getElementById(
                "page_metadata").innerHTML = JSON.stringify(response.data);
        }
    })
    .catch(function (error) {
        console.log(error);
        app.$data.alerts.push({
            text: "cannot load dataset record: [" + error +" (" + path + ")]",
            type: "error"
        });
    });
}

// googlebot does not like JS functions with default args!
function push_msg(msg, type) {
    console.log(msg);
    app.$data.alerts.push({
        text: msg,
        type: type
    })
}

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
