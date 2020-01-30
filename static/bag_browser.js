class BagHandler {
  constructor() {
    this.payload = {};
  }

  createManifestLink(space, external_identifier, version) {
    const template = "{{ url_for('get_bag_metadata', space='SPACE', external_identifier='EXTERNAL_IDENTIFIER', version='VERSION') }}";

    return template.replace("SPACE", space).replace("EXTERNAL_IDENTIFIER", external_identifier).replace("VERSION", version);
  }

  createZipLink(space, external_identifier, version) {
    const template = "{{ url_for('get_bag_files', space='SPACE', external_identifier='EXTERNAL_IDENTIFIER', version='VERSION') }}";

    return template.replace("SPACE", space).replace("EXTERNAL_IDENTIFIER", external_identifier).replace("VERSION", version);
  }

  renderTable() {
    var old_tbody = document.getElementById("tbody__bags");

    var new_tbody = document.createElement("tbody");
    new_tbody.id = "tbody__bags";

    for (var i = 0; i < this.payload["bags"].length; i++) {
      var current_bag = this.payload["bags"][i];

      var row = new_tbody.insertRow(-1);

      var extIdentifier = row.insertCell(-1);
      extIdentifier.classList.add("external_identifier");
      extIdentifier.innerHTML = current_bag["external_identifier"];

      var fileCount = row.insertCell(-1);
      fileCount.classList.add("file_count");
      fileCount.innerHTML = current_bag["file_count_pretty"];

      var fileSize = row.insertCell(-1);
      fileSize.classList.add("file_size");
      fileSize.innerHTML = current_bag["file_size_pretty"];

      var dateCreated = row.insertCell(-1);
      dateCreated.classList.add("date_created");
      dateCreated.innerHTML = '<span title="' + current_bag["date_created"] + '">' + current_bag["date_created_pretty"] + "</span>";

      var version = row.insertCell(-1);
      version.classList.add("version");
      version.innerHTML = "v" + current_bag["version"];

      var download = row.insertCell(-1);
      download.classList.add("download");
      download.innerHTML = "<a href=\"" + this.createManifestLink(current_bag["space"], current_bag["external_identifier"], current_bag["version"]) + "\">manifest</a> / <a href=\"" + this.createZipLink(current_bag["space"], current_bag["external_identifier"], current_bag["version"]) + "\">zip</a>";
    }

    old_tbody.parentNode.replaceChild(new_tbody, old_tbody);

    if (this.payload["total_bags"] == 1) {
      document.getElementById("li__total_bags").innerHTML = "1 matching bag";
    } else {
      document.getElementById("li__total_bags").innerHTML = this.payload["total_bags"] + " matching bags";
    }

    if (this.payload["total_files"] == "1") {
      document.getElementById("li__total_file_count").innerHTML = "1 file";
    } else {
      document.getElementById("li__total_file_count").innerHTML = this.payload["total_file_count"] + " files";
    }

    document.getElementById("li__total_file_size").innerHTML = this.payload["total_file_size"] + " of data";

    // https://stackoverflow.com/a/1069840/1558022
    var old_file_stats = document.getElementById("total_file_stats");

    var new_file_stats = document.createElement("tbody");
    new_file_stats.id = "total_file_stats";

    var sortableFileStats = [];
    for (var extension in this.payload["file_stats"]) {
      sortableFileStats.push([extension, this.payload["file_stats"][extension]]);
    }

    sortableFileStats.sort(function(a, b) {
      return b[1] - a[1];
    });

    for (var i = 0; i < sortableFileStats.length; i++) {
      var extension = sortableFileStats[i][0];
      var count = sortableFileStats[i][1];
      var row = new_file_stats.insertRow(-1);

      var extensionCell = row.insertCell(-1);
      extensionCell.classList.add("file_extension");
      extensionCell.innerHTML = extension;

      var countCell = row.insertCell(-1);
      countCell.classList.add("file_tally_count");
      countCell.innerHTML = count;
    }

    old_file_stats.parentNode.replaceChild(new_file_stats, old_file_stats);
  }
}

/**
 * http://stackoverflow.com/a/10997390/11236
 */
function updateURLParameter(url, param, paramVal){
    var newAdditionalURL = "";
    var tempArray = url.split("?");
    var baseURL = tempArray[0];
    var additionalURL = tempArray[1];
    var temp = "";
    if (additionalURL) {
        tempArray = additionalURL.split("&");
        for (var i=0; i<tempArray.length; i++){
            if(tempArray[i].split('=')[0] != param){
                newAdditionalURL += temp + tempArray[i];
                temp = "&";
            }
        }
    }

    var rows_txt = temp + "" + param + "=" + paramVal;
    return baseURL + "?" + newAdditionalURL + rows_txt;
}

class QueryContext {
  constructor(space, external_identifier_prefix, date_created_before, date_created_after, page, page_size, bagHandler) {
    this.space = space;
    this.external_identifier_prefix = external_identifier_prefix;
    this.date_created_before = date_created_before;
    this.date_created_after = date_created_after;
    this.page = page;
    this.page_size = page_size;
    this.bagHandler = bagHandler;
  }

  changeExternalIdentifierPrefix(newPrefix) {
    this.external_identifier_prefix = newPrefix;
    this.updateResults();

    var newUrl = updateURLParameter(window.location.href, "prefix", newPrefix);
    history.pushState({"prefix": newPrefix}, "", newUrl);
  }

  changeDateCreatedBefore(newDateCreatedBefore) {
    this.date_created_before = newDateCreatedBefore;
    this.updateResults();

    var newUrl = updateURLParameter(window.location.href, "created_before", newDateCreatedBefore);
    history.pushState({"created_before": newDateCreatedBefore}, "", newUrl);
  }

  changeDateCreatedAfter(newDateCreatedAfter) {
    this.date_created_after = newDateCreatedAfter;
    this.updateResults();

    var newUrl = updateURLParameter(window.location.href, "created_after", newDateCreatedAfter);
    history.pushState({"created_after": newDateCreatedAfter}, "", newUrl);
  }

  updateResults() {
    var xhttp = new XMLHttpRequest();

    // Extract it as a variable here -- inside onreadystatechange, this
    // refers to the response, not the QueryContext.
    bagHandler = this.bagHandler;

    xhttp.onreadystatechange = function() {
      if (this.readyState == 4 && this.status == 200) {
        bagHandler.payload = JSON.parse(this.responseText);
        bagHandler.renderTable();
      }
    };
    xhttp.open(
      "GET",
      "/spaces/" + this.space + "/get_bags_data?prefix=" + this.external_identifier_prefix + "&page=" + this.page + "&created_before=" + this.date_created_before + "&created_after=" + this.date_created_after,
      true
    );
    xhttp.send();
  }
}

function nextPage(page) {
  window.location.href = updateURLParameter(window.location.href, "page", page + 1);
}

function previousPage(page) {
  window.location.href = updateURLParameter(window.location.href, "page", page - 1);
}
