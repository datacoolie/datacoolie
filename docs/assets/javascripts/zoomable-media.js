(function () {
  let listenersAttached = false;
  let observerStarted = false;

  function collectMatches(root, selector) {
    const matches = [];
    const scope = root || document;

    if (scope.nodeType === Node.ELEMENT_NODE && scope.matches && scope.matches(selector)) {
      matches.push(scope);
    }

    if (scope.querySelectorAll) {
      matches.push(...scope.querySelectorAll(selector));
    }

    return Array.from(new Set(matches));
  }

  function createLightbox() {
    const existing = document.querySelector(".dc-media-lightbox");
    if (existing) {
      return existing;
    }

    const overlay = document.createElement("div");
    overlay.className = "dc-media-lightbox";
    overlay.setAttribute("aria-hidden", "true");

    const surface = document.createElement("div");
    surface.className = "dc-media-lightbox__surface";
    surface.setAttribute("role", "dialog");
    surface.setAttribute("aria-modal", "true");
    surface.setAttribute("aria-label", "Expanded image or diagram");

    const close = document.createElement("button");
    close.className = "dc-media-lightbox__close";
    close.type = "button";
    close.setAttribute("aria-label", "Close expanded view");
    close.textContent = "Close";

    const content = document.createElement("div");
    content.className = "dc-media-lightbox__content";

    const hint = document.createElement("div");
    hint.className = "dc-media-lightbox__hint";
    hint.textContent = "Press Esc or click outside to close";

    surface.appendChild(close);
    surface.appendChild(content);
    surface.appendChild(hint);
    overlay.appendChild(surface);
    document.body.appendChild(overlay);

    function closeLightbox() {
      overlay.classList.remove("is-open");
      overlay.setAttribute("aria-hidden", "true");
      content.replaceChildren();
      document.body.classList.remove("dc-media-lightbox-open");
    }

    close.addEventListener("click", closeLightbox);
    overlay.addEventListener("click", function (event) {
      if (event.target === overlay) {
        closeLightbox();
      }
    });

    document.addEventListener("keydown", function (event) {
      if (event.key === "Escape" && overlay.classList.contains("is-open")) {
        closeLightbox();
      }
    });

    overlay.openWith = function (node) {
      content.replaceChildren();
      content.appendChild(node);
      overlay.classList.add("is-open");
      overlay.setAttribute("aria-hidden", "false");
      document.body.classList.add("dc-media-lightbox-open");

      requestAnimationFrame(function () {
        surface.scrollTop = 0;
        surface.scrollLeft = Math.max(0, (surface.scrollWidth - surface.clientWidth) / 2);
      });
    };

    return overlay;
  }

  function serializeSvgToDataUrl(svg) {
    const clone = svg.cloneNode(true);
    clone.removeAttribute("width");
    clone.removeAttribute("height");
    clone.setAttribute("xmlns", "http://www.w3.org/2000/svg");
    clone.setAttribute("xmlns:xhtml", "http://www.w3.org/1999/xhtml");
    return `data:image/svg+xml;charset=utf-8,${encodeURIComponent(new XMLSerializer().serializeToString(clone))}`;
  }

  function cloneForLightbox(element) {
    const tagName = element.tagName ? element.tagName.toUpperCase() : "";
    if (tagName === "IMG") {
      const expanded = document.createElement("img");
      expanded.src = element.currentSrc || element.src;
      expanded.alt = element.alt || "Expanded documentation image";
      return expanded;
    }

    const svgEl = tagName === "SVG" ? element : element.querySelector("svg");
    if (!svgEl) {
      return null;
    }

    const sourceRect = svgEl.getBoundingClientRect();
    const targetWidth = Math.min(
      window.innerWidth * 0.98,
      Math.max(sourceRect.width * 2.0, sourceRect.width + 180)
    );

    const expanded = document.createElement("img");
    expanded.src = serializeSvgToDataUrl(svgEl);
    expanded.alt = "Expanded documentation diagram";
    expanded.decoding = "async";

    const bodyStyles = getComputedStyle(document.body);
    const frame = document.createElement("div");
    frame.style.display = "flex";
    frame.style.alignItems = "center";
    frame.style.justifyContent = "center";
    frame.style.width = `${targetWidth}px`;
    frame.style.maxWidth = "98vw";
    frame.style.maxHeight = "86vh";
    frame.style.backgroundColor = bodyStyles.getPropertyValue("--md-default-bg-color").trim() || bodyStyles.backgroundColor;

    frame.appendChild(expanded);
    return frame;
  }

  function getZoomTarget(node) {
    if (!node || !node.closest) {
      return null;
    }

    const image = node.closest(".md-content article img.dc-zoomable-media");
    if (image) {
      return image;
    }

    const svg = node.closest(".md-content article .mermaid.dc-zoomable-media svg");
    if (svg) {
      return svg;
    }

    return node.closest(".md-content article .mermaid.dc-zoomable-media");
  }

  function decorateZoomTargets(root) {
    collectMatches(root, ".md-content article img, .md-content article .mermaid").forEach(function (element) {
      element.classList.add("dc-zoomable-media");
      element.setAttribute("tabindex", "0");
      element.setAttribute("role", "button");

      if (!element.hasAttribute("aria-label")) {
        const tagName = element.tagName ? element.tagName.toUpperCase() : "";
        element.setAttribute(
          "aria-label",
          tagName === "IMG" ? "Open enlarged image" : "Open enlarged diagram"
        );
      }
    });
  }

  function openZoomTarget(target) {
    const expanded = cloneForLightbox(target);
    if (!expanded) {
      return;
    }

    createLightbox().openWith(expanded);
  }

  function initZoomableMedia() {
    decorateZoomTargets(document);

    if (!listenersAttached) {
      listenersAttached = true;

      document.addEventListener(
        "click",
        function (event) {
          if (event.target.closest(".dc-media-lightbox")) {
            return;
          }

          const target = getZoomTarget(event.target);
          if (!target) {
            return;
          }

          event.preventDefault();
          event.stopPropagation();
          openZoomTarget(target);
        },
        true
      );

      document.addEventListener("keydown", function (event) {
        if (event.key !== "Enter" && event.key !== " ") {
          return;
        }

        const target = getZoomTarget(document.activeElement);
        if (!target) {
          return;
        }

        event.preventDefault();
        openZoomTarget(target);
      });

      document.addEventListener("dc:zoomable-media:refresh", function () {
        decorateZoomTargets(document);
      });
    }

    if (!observerStarted) {
      observerStarted = true;

      const observer = new MutationObserver(function (mutations) {
        mutations.forEach(function (mutation) {
          mutation.addedNodes.forEach(function (node) {
            if (node.nodeType === Node.ELEMENT_NODE) {
              decorateZoomTargets(node);
            }
          });
        });
      });

      observer.observe(document.body, { childList: true, subtree: true });
    }
  }

  if (typeof document$ !== "undefined" && document$.subscribe) {
    document$.subscribe(function () {
      initZoomableMedia();
    });
  } else {
    document.addEventListener("DOMContentLoaded", initZoomableMedia, { once: true });
  }
})();