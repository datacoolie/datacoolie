(function () {
  "use strict";

  const COLLAPSED_HEIGHT = 24 * 16;
  let resizeTimer = null;
  let nextId = 0;

  function onExamplesPage() {
    return /(?:^|\/)examples(?:\/|$)/.test(window.location.pathname);
  }

  function isSourceViewPage() {
    return /(?:^|\/)examples\/source(?:\/|$)/.test(window.location.pathname);
  }

  function codeBlocks(root) {
    const scope = root && root.querySelectorAll ? root : document;
    return Array.from(scope.querySelectorAll(".md-content article pre"));
  }

  function setButtonState(pre, button, expanded) {
    pre.classList.toggle("dc-example-code--collapsed", !expanded);
    button.setAttribute("aria-expanded", expanded ? "true" : "false");
    button.textContent = expanded ? "Collapse code" : "Expand code";
  }

  function addToggle(pre) {
    if (pre._dcExampleCodeToggle || !pre.querySelector("code")) {
      return pre._dcExampleCodeToggle || null;
    }

    const generatedId = !pre.id;
    const id = pre.id || "dc-example-code-" + (++nextId);
    pre.id = id;
    pre._dcExampleCodeGeneratedId = generatedId;
    const button = document.createElement("button");
    button.type = "button";
    button.className = "dc-example-code-toggle";
    button.setAttribute("aria-controls", id);
    button.setAttribute("aria-expanded", "false");
    button.textContent = "Expand code";
    button.addEventListener("click", function () {
      const expanded = button.getAttribute("aria-expanded") === "true";
      setButtonState(pre, button, !expanded);
    });

    const host = pre.closest(".highlight") || pre.parentElement;
    if (!host) {
      return null;
    }
    host.insertAdjacentElement("afterend", button);
    pre._dcExampleCodeToggle = button;
    setButtonState(pre, button, false);
    return button;
  }

  function removeToggle(pre) {
    const button = pre._dcExampleCodeToggle;
    if (button && button.parentNode) {
      button.parentNode.removeChild(button);
    }
    pre._dcExampleCodeToggle = null;
    pre.classList.remove("dc-example-code--collapsed");
    if (pre._dcExampleCodeGeneratedId) {
      delete pre.id;
    }
    pre._dcExampleCodeGeneratedId = false;
  }

  function refresh(root) {
    if (!onExamplesPage()) {
      return;
    }
    const blocks = codeBlocks(root);
    // Dedicated source views are already complete readable projections. Remove
    // any stale control left by instant navigation before returning.
    if (isSourceViewPage()) {
      blocks.forEach(function (pre) {
        if (pre._dcExampleCodeToggle) {
          removeToggle(pre);
        }
      });
      return;
    }
    // Keep the compact control for code embedded in guides, where it protects
    // the surrounding narrative from one very long block.
    blocks.forEach(function (pre) {
      const tooTall = pre.scrollHeight > COLLAPSED_HEIGHT + 8;
      if (tooTall) {
        addToggle(pre);
      } else if (pre._dcExampleCodeToggle) {
        removeToggle(pre);
      }
    });
  }

  function scheduleRefresh() {
    if (resizeTimer !== null) {
      window.clearTimeout(resizeTimer);
    }
    resizeTimer = window.setTimeout(function () {
      resizeTimer = null;
      refresh(document);
    }, 120);
  }

  function revealHashTarget() {
    if (!window.location.hash) {
      return;
    }
    let id;
    try {
      id = decodeURIComponent(window.location.hash.slice(1));
    } catch (_error) {
      return;
    }
    const target = document.getElementById(id);
    const pre = target && target.closest ? target.closest("pre") : null;
    const button = pre && pre._dcExampleCodeToggle;
    if (button && button.getAttribute("aria-expanded") !== "true") {
      button.click();
    }
  }

  function init() {
    refresh(document);
    revealHashTarget();
    if (!window._dcExampleCodeResizeAttached) {
      window._dcExampleCodeResizeAttached = true;
      window.addEventListener("resize", scheduleRefresh, { passive: true });
      window.addEventListener("hashchange", revealHashTarget);
    }
  }

  if (typeof document$ !== "undefined" && document$.subscribe) {
    document$.subscribe(init);
  } else {
    document.addEventListener("DOMContentLoaded", init, { once: true });
  }
})();
