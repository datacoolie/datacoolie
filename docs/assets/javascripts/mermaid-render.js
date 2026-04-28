(function () {
  let mermaidConfigured = false;
  let observerStarted = false;
  let themeObserverStarted = false;
  let lastThemeSignature = "";

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

  function readCssVar(styles, name, fallback) {
    const value = styles.getPropertyValue(name).trim();
    return value || fallback;
  }

  function getThemeSignature() {
    if (!document.body) {
      return "";
    }

    const styles = getComputedStyle(document.body);
    return [
      document.body.dataset.mdColorScheme || "default",
      readCssVar(styles, "--md-default-bg-color", ""),
      readCssVar(styles, "--md-typeset-color", ""),
      readCssVar(styles, "--md-default-fg-color--lighter", ""),
      readCssVar(styles, "--md-default-fg-color--lightest", ""),
      readCssVar(styles, "--md-code-bg-color", ""),
      readCssVar(styles, "--md-accent-fg-color", ""),
    ].join("|");
  }

  function getThemePalette() {
    const styles = getComputedStyle(document.body);
    const scheme = document.body.dataset.mdColorScheme || "default";

    return {
      scheme: scheme,
      background: readCssVar(styles, "--md-default-bg-color", scheme === "slate" ? "#0f172a" : "#ffffff"),
      textColor: readCssVar(styles, "--md-typeset-color", scheme === "slate" ? "rgba(255,255,255,0.86)" : "rgba(0,0,0,0.87)"),
      borderColor: readCssVar(styles, "--md-default-fg-color--lighter", scheme === "slate" ? "rgba(255,255,255,0.32)" : "rgba(0,0,0,0.32)"),
      subtleFill: readCssVar(styles, "--md-default-fg-color--lightest", scheme === "slate" ? "rgba(255,255,255,0.08)" : "rgba(0,0,0,0.06)"),
      codeBackground: readCssVar(styles, "--md-code-bg-color", scheme === "slate" ? "#111827" : "#f8fafc"),
      accent: readCssVar(styles, "--md-accent-fg-color", readCssVar(styles, "--md-primary-fg-color", "#4f46e5")),
      fontFamily: readCssVar(styles, "--md-text-font-family", getComputedStyle(document.body).fontFamily),
    };
  }

  function getMermaidConfig() {
    const palette = getThemePalette();
    const scheme = palette.scheme;
    const background = palette.background;
    const textColor = palette.textColor;
    const borderColor = palette.borderColor;
    const subtleFill = palette.subtleFill;
    const codeBackground = palette.codeBackground;
    const accent = palette.accent;
    const fontFamily = palette.fontFamily;

    return {
      startOnLoad: false,
      securityLevel: "loose",
      theme: "base",
      fontFamily: fontFamily,
      themeVariables: {
        darkMode: scheme === "slate",
        fontFamily: fontFamily,
        background: background,
        textColor: textColor,
        lineColor: borderColor,
        primaryColor: background,
        primaryTextColor: textColor,
        primaryBorderColor: borderColor,
        secondaryColor: subtleFill,
        secondaryTextColor: textColor,
        secondaryBorderColor: borderColor,
        tertiaryColor: codeBackground,
        tertiaryTextColor: textColor,
        tertiaryBorderColor: borderColor,
        mainBkg: background,
        secondBkg: subtleFill,
        tertiaryBkg: codeBackground,
        nodeBorder: borderColor,
        clusterBkg: subtleFill,
        clusterBorder: borderColor,
        defaultLinkColor: borderColor,
        edgeLabelBackground: background,
        titleColor: textColor,
        actorBkg: background,
        actorBorder: borderColor,
        actorTextColor: textColor,
        labelBoxBkgColor: background,
        labelBoxBorderColor: borderColor,
        labelTextColor: textColor,
        loopTextColor: textColor,
        signalColor: borderColor,
        signalTextColor: textColor,
        noteBkg: codeBackground,
        noteBorderColor: borderColor,
        noteTextColor: textColor,
        activationBorderColor: borderColor,
        activationBkgColor: subtleFill,
        sequenceNumberColor: textColor,
        cScale0: accent,
        cScale1: subtleFill,
        cScale2: codeBackground,
        cScale3: borderColor,
        cScale4: background,
        cScale5: accent,
        pie1: accent,
        pie2: borderColor,
        pie3: subtleFill,
        pie4: codeBackground,
        pie5: background,
      },
      themeCSS: [
        ".label, .label text, .nodeLabel, .edgeLabel, .cluster-label text, .titleText, .taskText, .sectionTitle, .messageText, .loopText, .noteText { fill: " + textColor + " !important; color: " + textColor + " !important; font-family: " + fontFamily + " !important; }",
        ".edgeLabel rect, .labelBkg { fill: " + background + " !important; }",
        ".cluster rect, .cluster polygon, .cluster path { fill: " + subtleFill + " !important; stroke: " + borderColor + " !important; }",
        ".note rect, .note polygon, .note path { fill: " + codeBackground + " !important; stroke: " + borderColor + " !important; }",
        ".node rect, .node circle, .node ellipse, .node polygon, .node path, .actor, .actor-top, .actor-bottom, .labelBox { stroke: " + borderColor + " !important; }",
        ".edgePath .path, .flowchart-link, .messageLine0, .messageLine1, .actor-line, .loopLine { stroke: " + borderColor + " !important; }",
      ].join("\n"),
      sequence: {
        actorFontSize: "16px",
        messageFontSize: "16px",
        noteFontSize: "16px",
      },
    };
  }

  function applyInlineStyles(elements, styles) {
    elements.forEach(function (element) {
      Object.keys(styles).forEach(function (key) {
        element.style[key] = styles[key];
      });
    });
  }

  function styleRenderedDiagrams() {
    if (!document.body) {
      return;
    }

    const palette = getThemePalette();

    collectMatches(document, ".md-content article .mermaid svg").forEach(function (svg) {
      svg.style.maxWidth = "100%";
      svg.style.height = "auto";
      svg.style.color = palette.textColor;

      applyInlineStyles(
        svg.querySelectorAll("text, tspan, .nodeLabel, .label, .cluster-label text, .messageText, .loopText, .noteText"),
        {
          fill: palette.textColor,
          color: palette.textColor,
          fontFamily: palette.fontFamily,
        }
      );

      applyInlineStyles(
        svg.querySelectorAll("foreignObject, foreignObject div, foreignObject span, foreignObject p, .labelBkg"),
        {
          color: palette.textColor,
          fontFamily: palette.fontFamily,
        }
      );

      applyInlineStyles(
        svg.querySelectorAll("foreignObject div, foreignObject span, foreignObject p, .labelBkg"),
        {
          backgroundColor: "transparent",
          borderColor: "transparent",
        }
      );

      applyInlineStyles(
        svg.querySelectorAll(".node rect, .node circle, .node ellipse, .node polygon, .node path, .actor, .actor-top, .actor-bottom, .labelBox, path.label-container, path.basic, rect.basic, polygon.basic, ellipse.basic, circle.basic"),
        {
          fill: palette.subtleFill,
          stroke: palette.borderColor,
        }
      );

      applyInlineStyles(
        svg.querySelectorAll(".cluster rect, .cluster polygon, .cluster path"),
        {
          fill: palette.codeBackground,
          stroke: palette.borderColor,
        }
      );

      applyInlineStyles(
        svg.querySelectorAll(".note rect, .note polygon, .note path, rect.note, polygon.note, path.note"),
        {
          fill: palette.codeBackground,
          stroke: palette.borderColor,
        }
      );

      applyInlineStyles(
        svg.querySelectorAll(".edgePath .path, .flowchart-link, .edgePaths path, .messageLine0, .messageLine1, .actor-line, .loopLine"),
        {
          stroke: palette.borderColor,
          fill: "none",
        }
      );

      applyInlineStyles(
        svg.querySelectorAll(".arrowheadPath, .arrowMarkerPath, marker path, .marker path"),
        {
          stroke: palette.borderColor,
          fill: palette.borderColor,
        }
      );

      applyInlineStyles(
        svg.querySelectorAll(".edgeLabel rect, .labelBkg"),
        {
          fill: "transparent",
          stroke: "none",
        }
      );
    });

    document.dispatchEvent(new CustomEvent("dc:zoomable-media:refresh"));
  }

  function ensureMermaidConfigured(force) {
    if (typeof mermaid === "undefined" || !document.body) {
      return false;
    }

    const themeSignature = getThemeSignature();

    if (force || !mermaidConfigured || lastThemeSignature !== themeSignature) {
      mermaid.initialize(getMermaidConfig());
      mermaidConfigured = true;
      lastThemeSignature = themeSignature;
    }

    return true;
  }

  function prepareSourceBlocks(root) {
    collectMatches(root, "pre.dc-mermaid-source").forEach(function (block) {
      if (block.dataset.dcMermaidPrepared === "true") {
        return;
      }

      const code = block.querySelector("code");
      const source = code ? code.textContent : block.textContent;
      if (!source || !source.trim()) {
        return;
      }

      const container = document.createElement("div");
      container.className = "mermaid";
      container.dataset.dcMermaidSource = source.trim();
      container.textContent = source.trim();

      block.dataset.dcMermaidPrepared = "true";
      block.replaceWith(container);
    });
  }

  function resetRenderedDiagrams() {
    collectMatches(document, ".md-content article .mermaid").forEach(function (node) {
      const source = node.dataset.dcMermaidSource || "";
      if (!source) {
        return;
      }

      node.removeAttribute("data-processed");
      node.replaceChildren();
      node.textContent = source;
    });
  }

  function renderMermaid(root, forceReset) {
    prepareSourceBlocks(root || document);

    if (!ensureMermaidConfigured(forceReset)) {
      return;
    }

    if (forceReset) {
      resetRenderedDiagrams();
    }

    const nodes = collectMatches(document, ".md-content article .mermaid").filter(function (node) {
      const source = node.dataset.dcMermaidSource || (node.textContent || "").trim();
      return Boolean(source) && (forceReset || !node.querySelector("svg"));
    });

    if (!nodes.length) {
      return;
    }

    mermaid.run({ nodes: nodes }).then(function () {
      styleRenderedDiagrams();
    }).catch(function (error) {
      console.error("Failed to render Mermaid diagrams", error);
    });
  }

  function scheduleRender(root, forceReset) {
    setTimeout(function () {
      renderMermaid(root || document, forceReset);
    }, 0);

    setTimeout(function () {
      renderMermaid(root || document, forceReset);
    }, 250);
  }

  function observeThemeChanges() {
    if (themeObserverStarted || !document.body) {
      return;
    }

    themeObserverStarted = true;

    const observer = new MutationObserver(function (mutations) {
      if (!mutations.some(function (mutation) {
        return mutation.attributeName === "data-md-color-scheme";
      })) {
        return;
      }

      const nextThemeSignature = getThemeSignature();
      if (nextThemeSignature === lastThemeSignature) {
        return;
      }

      requestAnimationFrame(function () {
        scheduleRender(document, true);
        setTimeout(styleRenderedDiagrams, 80);
      });
    });

    observer.observe(document.body, {
      attributes: true,
      attributeFilter: ["data-md-color-scheme"],
    });
  }

  function initMermaidRendering() {
    observeThemeChanges();
    scheduleRender(document);
    setTimeout(styleRenderedDiagrams, 120);

    if (observerStarted) {
      return;
    }

    observerStarted = true;

    const observer = new MutationObserver(function (mutations) {
      mutations.forEach(function (mutation) {
        mutation.addedNodes.forEach(function (node) {
          if (node.nodeType === Node.ELEMENT_NODE) {
            scheduleRender(node);
            setTimeout(styleRenderedDiagrams, 120);
          }
        });
      });
    });

    observer.observe(document.body, { childList: true, subtree: true });
  }

  if (document.body) {
    ensureMermaidConfigured(true);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initMermaidRendering, { once: true });
  } else {
    initMermaidRendering();
  }

  if (typeof document$ !== "undefined" && document$.subscribe) {
    document$.subscribe(function () {
      initMermaidRendering();
    });
  }
})();