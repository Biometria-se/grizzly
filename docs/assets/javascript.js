document.addEventListener("DOMContentLoaded", (_) => {
    dayjs.extend(window.dayjs_plugin_relativeTime);

    // <!-- update page frontmatter to human readable relative time since updated
    let fact_element = document.querySelector("span.md-source-file__fact");
    if (fact_element) {
        let date_updated = fact_element.getAttribute("data-updated");
        let date_element = document.getElementById("md-source-file__fact-updated");
        if (date_element && date_updated) {
            let relative_date = dayjs(date_updated).fromNow();
            date_element.innerText = relative_date;
        }
        // -->
    }
});
