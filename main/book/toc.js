// Populate the sidebar
//
// This is a script, and not included directly in the page, to control the total size of the book.
// The TOC contains an entry for each page, so if each page includes a copy of the TOC,
// the total size of the page becomes O(n**2).
class MDBookSidebarScrollbox extends HTMLElement {
    constructor() {
        super();
    }
    connectedCallback() {
        this.innerHTML = '<ol class="chapter"><li class="chapter-item expanded affix "><a href="introduction.html">Introduction</a></li><li class="chapter-item expanded affix "><a href="installation.html">Installation</a></li><li class="chapter-item expanded affix "><a href="faq.html">FAQ</a></li><li class="chapter-item expanded affix "><li class="part-title">Quickstart Guides</li><li class="chapter-item expanded "><a href="deployment/quickstarts/netcat.html"><strong aria-hidden="true">1.</strong> Netcat</a></li><li class="chapter-item expanded "><a href="deployment/quickstarts/agones-xonotic-sidecar.html"><strong aria-hidden="true">2.</strong> Agones + Xonotic (Sidecar)</a></li><li class="chapter-item expanded "><a href="deployment/quickstarts/agones-xonotic-xds.html"><strong aria-hidden="true">3.</strong> Agones + Xonotic (xDS)</a></li><li class="chapter-item expanded "><a href="deployment/quickstarts/agones-xonotic-relay.html"><strong aria-hidden="true">4.</strong> Agones + Xonotic (Relay)</a></li><li class="chapter-item expanded affix "><li class="part-title">Services</li><li class="chapter-item expanded "><a href="services/proxy.html"><strong aria-hidden="true">5.</strong> Proxy</a></li><li><ol class="section"><li class="chapter-item expanded "><a href="services/proxy/configuration.html"><strong aria-hidden="true">5.1.</strong> Configuration File</a></li><li class="chapter-item expanded "><a href="services/proxy/filters.html"><strong aria-hidden="true">5.2.</strong> Filters</a></li><li><ol class="section"><li class="chapter-item expanded "><a href="services/proxy/filters/capture.html"><strong aria-hidden="true">5.2.1.</strong> Capture</a></li><li class="chapter-item expanded "><a href="services/proxy/filters/concatenate.html"><strong aria-hidden="true">5.2.2.</strong> Concatenate</a></li><li class="chapter-item expanded "><a href="services/proxy/filters/debug.html"><strong aria-hidden="true">5.2.3.</strong> Debug</a></li><li class="chapter-item expanded "><a href="services/proxy/filters/drop.html"><strong aria-hidden="true">5.2.4.</strong> Drop</a></li><li class="chapter-item expanded "><a href="services/proxy/filters/firewall.html"><strong aria-hidden="true">5.2.5.</strong> Firewall</a></li><li class="chapter-item expanded "><a href="services/proxy/filters/load_balancer.html"><strong aria-hidden="true">5.2.6.</strong> Load Balancer</a></li><li class="chapter-item expanded "><a href="services/proxy/filters/local_rate_limit.html"><strong aria-hidden="true">5.2.7.</strong> Local Rate Limit</a></li><li class="chapter-item expanded "><a href="services/proxy/filters/match.html"><strong aria-hidden="true">5.2.8.</strong> Match</a></li><li class="chapter-item expanded "><a href="services/proxy/filters/pass.html"><strong aria-hidden="true">5.2.9.</strong> Pass</a></li><li class="chapter-item expanded "><a href="services/proxy/filters/timestamp.html"><strong aria-hidden="true">5.2.10.</strong> Timestamp</a></li><li class="chapter-item expanded "><a href="services/proxy/filters/token_router.html"><strong aria-hidden="true">5.2.11.</strong> Token Router</a></li></ol></li><li class="chapter-item expanded "><a href="services/proxy/qcmp.html"><strong aria-hidden="true">5.3.</strong> Control Message Protocol</a></li><li class="chapter-item expanded "><a href="services/proxy/metrics.html"><strong aria-hidden="true">5.4.</strong> Metrics</a></li></ol></li><li class="chapter-item expanded "><li class="spacer"></li><li class="chapter-item expanded "><a href="services/xds.html"><strong aria-hidden="true">6.</strong> Control Plane</a></li><li><ol class="section"><li class="chapter-item expanded "><div><strong aria-hidden="true">6.1.</strong> Providers</div></li><li><ol class="section"><li class="chapter-item expanded "><a href="services/xds/providers/agones.html"><strong aria-hidden="true">6.1.1.</strong> Agones</a></li><li class="chapter-item expanded "><a href="services/xds/providers/filesystem.html"><strong aria-hidden="true">6.1.2.</strong> Filesystem</a></li></ol></li><li class="chapter-item expanded "><a href="services/xds/proto/index.html"><strong aria-hidden="true">6.2.</strong> Protobuf Reference</a></li></ol></li><li class="chapter-item expanded "><li class="spacer"></li><li class="chapter-item expanded "><a href="services/relay.html"><strong aria-hidden="true">7.</strong> Relay</a></li><li><ol class="section"><li class="chapter-item expanded "><div><strong aria-hidden="true">7.1.</strong> Metrics</div></li><li class="chapter-item expanded "><a href="services/agent.html"><strong aria-hidden="true">7.2.</strong> Agents</a></li></ol></li><li class="chapter-item expanded "><li class="part-title">SDKs</li><li class="chapter-item expanded "><a href="sdks/unreal-engine.html"><strong aria-hidden="true">8.</strong> Unreal Engine</a></li><li class="chapter-item expanded affix "><li class="part-title">Deployment</li><li class="chapter-item expanded "><a href="deployment/admin.html"><strong aria-hidden="true">9.</strong> Administration</a></li><li class="chapter-item expanded "><a href="deployment/examples.html"><strong aria-hidden="true">10.</strong> Architecture Examples</a></li><li class="chapter-item expanded affix "><li class="part-title">Third Party</li><li class="chapter-item expanded "><a href="third-party/presentations.html"><strong aria-hidden="true">11.</strong> Videos and Presentations</a></li></ol>';
        // Set the current, active page, and reveal it if it's hidden
        let current_page = document.location.href.toString().split("#")[0].split("?")[0];
        if (current_page.endsWith("/")) {
            current_page += "index.html";
        }
        var links = Array.prototype.slice.call(this.querySelectorAll("a"));
        var l = links.length;
        for (var i = 0; i < l; ++i) {
            var link = links[i];
            var href = link.getAttribute("href");
            if (href && !href.startsWith("#") && !/^(?:[a-z+]+:)?\/\//.test(href)) {
                link.href = path_to_root + href;
            }
            // The "index" page is supposed to alias the first chapter in the book.
            if (link.href === current_page || (i === 0 && path_to_root === "" && current_page.endsWith("/index.html"))) {
                link.classList.add("active");
                var parent = link.parentElement;
                if (parent && parent.classList.contains("chapter-item")) {
                    parent.classList.add("expanded");
                }
                while (parent) {
                    if (parent.tagName === "LI" && parent.previousElementSibling) {
                        if (parent.previousElementSibling.classList.contains("chapter-item")) {
                            parent.previousElementSibling.classList.add("expanded");
                        }
                    }
                    parent = parent.parentElement;
                }
            }
        }
        // Track and set sidebar scroll position
        this.addEventListener('click', function(e) {
            if (e.target.tagName === 'A') {
                sessionStorage.setItem('sidebar-scroll', this.scrollTop);
            }
        }, { passive: true });
        var sidebarScrollTop = sessionStorage.getItem('sidebar-scroll');
        sessionStorage.removeItem('sidebar-scroll');
        if (sidebarScrollTop) {
            // preserve sidebar scroll position when navigating via links within sidebar
            this.scrollTop = sidebarScrollTop;
        } else {
            // scroll sidebar to current active section when navigating via "next/previous chapter" buttons
            var activeSection = document.querySelector('#sidebar .active');
            if (activeSection) {
                activeSection.scrollIntoView({ block: 'center' });
            }
        }
        // Toggle buttons
        var sidebarAnchorToggles = document.querySelectorAll('#sidebar a.toggle');
        function toggleSection(ev) {
            ev.currentTarget.parentElement.classList.toggle('expanded');
        }
        Array.from(sidebarAnchorToggles).forEach(function (el) {
            el.addEventListener('click', toggleSection);
        });
    }
}
window.customElements.define("mdbook-sidebar-scrollbox", MDBookSidebarScrollbox);
