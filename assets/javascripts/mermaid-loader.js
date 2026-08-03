(function () {
  const loaderScript = document.currentScript;
  const assetBase = loaderScript
    ? new URL(".", loaderScript.src).href
    : new URL("assets/javascripts/", document.baseURI).href;
  let loadPromise = null;

  function pageNeedsMermaid() {
    return Boolean(
      document.querySelector(".md-content article pre.dc-mermaid-source, .md-content article .mermaid")
    );
  }

  function loadScript(fileName) {
    const src = new URL(fileName, assetBase).href;
    const existing = Array.from(document.scripts).find(function (script) {
      return script.src === src;
    });

    if (existing) {
      if (existing.dataset.dcLoaded === "true") {
        return Promise.resolve();
      }

      return new Promise(function (resolve, reject) {
        existing.addEventListener("load", resolve, { once: true });
        existing.addEventListener("error", reject, { once: true });
      });
    }

    return new Promise(function (resolve, reject) {
      const script = document.createElement("script");
      script.src = src;
      script.defer = true;
      script.addEventListener(
        "load",
        function () {
          script.dataset.dcLoaded = "true";
          resolve();
        },
        { once: true }
      );
      script.addEventListener(
        "error",
        function (error) {
          script.remove();
          reject(error);
        },
        { once: true }
      );
      document.head.appendChild(script);
    });
  }

  function ensureMermaid() {
    if (!pageNeedsMermaid()) {
      return;
    }

    if (!loadPromise) {
      loadPromise = loadScript("mermaid.min.js")
        .then(function () {
          return loadScript("mermaid-render.js");
        })
        .catch(function (error) {
          loadPromise = null;
          console.error("Failed to load Mermaid", error);
        });
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", ensureMermaid, { once: true });
  } else {
    ensureMermaid();
  }

  if (typeof document$ !== "undefined" && document$.subscribe) {
    document$.subscribe(ensureMermaid);
  }
})();
