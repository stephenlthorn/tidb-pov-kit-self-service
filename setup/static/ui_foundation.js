(function () {
  const shell = document.getElementById("app_shell");
  const contextGrid = document.getElementById("workspace_grid");
  const contextPanel = document.getElementById("context_panel");

  const navCollapseBtn = document.getElementById("nav_collapse_btn");
  const contextCollapseBtn = document.getElementById("context_collapse_btn");
  const themeToggleBtn = document.getElementById("theme_toggle_btn");

  const commandBackdrop = document.getElementById("command_backdrop");
  const commandBar = document.getElementById("command_bar");
  const commandInput = document.getElementById("command_input");
  const commandItems = Array.from(document.querySelectorAll("[data-command-item]"));

  const modalBackdrop = document.getElementById("modal_backdrop");
  const modal = document.getElementById("confirm_modal");
  const clearDataBtn = document.getElementById("clear_data_btn");
  const modalCancelBtn = document.getElementById("modal_cancel_btn");
  const modalConfirmBtn = document.getElementById("modal_confirm_btn");

  const runButton = document.getElementById("run_scenario_btn");
  const runState = document.getElementById("run_state");
  const runLoading = document.getElementById("run_loading");

  const tabButtons = Array.from(document.querySelectorAll("[role='tab']"));
  const tabPanels = Array.from(document.querySelectorAll("[role='tabpanel']"));

  const toasts = document.getElementById("toast_region");

  let commandIndex = -1;

  function showToast(message, tone) {
    const node = document.createElement("div");
    node.className = "toast";
    node.textContent = message;
    if (tone === "danger") {
      node.style.borderColor = "var(--danger)";
    }
    if (tone === "success") {
      node.style.borderColor = "var(--success)";
    }
    toasts.appendChild(node);
    window.setTimeout(() => {
      node.remove();
    }, 3000);
  }

  function setTheme(theme) {
    document.documentElement.setAttribute("data-theme", theme);
    window.localStorage.setItem("ui-theme", theme);
    if (themeToggleBtn) {
      themeToggleBtn.textContent = theme === "dark" ? "Light" : "Dark";
      themeToggleBtn.setAttribute("aria-label", theme === "dark" ? "Switch to light theme" : "Switch to dark theme");
    }
  }

  function openCommandBar() {
    commandBackdrop.setAttribute("aria-hidden", "false");
    commandBar.setAttribute("aria-hidden", "false");
    commandIndex = -1;
    commandInput.value = "";
    setCommandVisibility("");
    commandInput.focus();
  }

  function closeCommandBar() {
    commandBackdrop.setAttribute("aria-hidden", "true");
    commandBar.setAttribute("aria-hidden", "true");
  }

  function setCommandVisibility(query) {
    const q = query.trim().toLowerCase();
    commandItems.forEach((item) => {
      const haystack = item.dataset.commandItem.toLowerCase();
      const visible = !q || haystack.includes(q);
      item.hidden = !visible;
      item.dataset.active = "false";
    });
    commandIndex = -1;
  }

  function setActiveCommand(next) {
    const visible = commandItems.filter((i) => !i.hidden);
    if (!visible.length) {
      commandIndex = -1;
      return;
    }
    commandIndex = Math.max(0, Math.min(next, visible.length - 1));
    visible.forEach((item, idx) => {
      item.dataset.active = idx === commandIndex ? "true" : "false";
    });
  }

  function executeCommand(item) {
    const action = item.dataset.commandAction;
    if (action === "run") {
      runButton.click();
    } else if (action === "toggle-theme") {
      themeToggleBtn.click();
    } else if (action === "open-modal") {
      openModal();
    } else {
      showToast("Command executed: " + item.dataset.commandItem, "success");
    }
    closeCommandBar();
  }

  function openModal() {
    modalBackdrop.setAttribute("aria-hidden", "false");
    modal.setAttribute("aria-hidden", "false");
    modalConfirmBtn.focus();
  }

  function closeModal() {
    modalBackdrop.setAttribute("aria-hidden", "true");
    modal.setAttribute("aria-hidden", "true");
  }

  function setRunning(running) {
    runButton.disabled = running;
    runLoading.hidden = !running;
    runState.textContent = running ? "Running checks..." : "Idle";
    runState.className = running ? "badge warning" : "badge";
  }

  function activateTab(tabId) {
    tabButtons.forEach((btn) => {
      const selected = btn.id === tabId;
      btn.setAttribute("aria-selected", selected ? "true" : "false");
      btn.tabIndex = selected ? 0 : -1;
    });
    tabPanels.forEach((panel) => {
      const active = panel.dataset.tabPanel === tabId;
      panel.classList.toggle("is-active", active);
      panel.hidden = !active;
    });
  }

  if (navCollapseBtn) {
    navCollapseBtn.addEventListener("click", () => {
      const next = shell.dataset.navCollapsed !== "true";
      shell.dataset.navCollapsed = String(next);
      showToast(next ? "Navigation collapsed" : "Navigation expanded", "success");
    });
  }

  if (contextCollapseBtn) {
    contextCollapseBtn.addEventListener("click", () => {
      const next = contextGrid.dataset.contextCollapsed !== "true";
      contextGrid.dataset.contextCollapsed = String(next);
      contextPanel.dataset.collapsed = String(next);
      contextCollapseBtn.textContent = next ? "Expand" : "Collapse";
    });
  }

  if (themeToggleBtn) {
    themeToggleBtn.addEventListener("click", () => {
      const current = document.documentElement.getAttribute("data-theme") || "dark";
      setTheme(current === "dark" ? "light" : "dark");
    });
  }

  if (commandInput) {
    commandInput.addEventListener("input", (e) => {
      setCommandVisibility(e.target.value);
    });
    commandInput.addEventListener("keydown", (e) => {
      const visible = commandItems.filter((i) => !i.hidden);
      if (e.key === "ArrowDown") {
        e.preventDefault();
        setActiveCommand(commandIndex + 1);
      }
      if (e.key === "ArrowUp") {
        e.preventDefault();
        setActiveCommand(commandIndex - 1);
      }
      if (e.key === "Enter" && commandIndex >= 0 && visible[commandIndex]) {
        e.preventDefault();
        executeCommand(visible[commandIndex]);
      }
    });
  }

  commandItems.forEach((item) => {
    item.addEventListener("click", () => executeCommand(item));
  });

  commandBackdrop.addEventListener("click", closeCommandBar);

  if (clearDataBtn) {
    clearDataBtn.addEventListener("click", openModal);
  }

  modalBackdrop.addEventListener("click", closeModal);
  modalCancelBtn.addEventListener("click", closeModal);
  modalConfirmBtn.addEventListener("click", () => {
    closeModal();
    showToast("Data reset queued.", "danger");
  });

  if (runButton) {
    runButton.addEventListener("click", () => {
      setRunning(true);
      showToast("Scenario started in background.", "success");
      window.setTimeout(() => setRunning(false), 1800);
    });
  }

  tabButtons.forEach((btn) => {
    btn.addEventListener("click", () => activateTab(btn.id));
    btn.addEventListener("keydown", (e) => {
      if (e.key !== "ArrowRight" && e.key !== "ArrowLeft") {
        return;
      }
      e.preventDefault();
      const idx = tabButtons.indexOf(btn);
      const delta = e.key === "ArrowRight" ? 1 : -1;
      const next = (idx + delta + tabButtons.length) % tabButtons.length;
      tabButtons[next].focus();
      activateTab(tabButtons[next].id);
    });
  });

  document.addEventListener("keydown", (e) => {
    const isMetaK = (e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k";
    if (isMetaK) {
      e.preventDefault();
      openCommandBar();
      return;
    }
    if (e.key === "Escape") {
      closeCommandBar();
      closeModal();
    }
  });

  const initialTheme = window.localStorage.getItem("ui-theme") || "dark";
  setTheme(initialTheme);
  activateTab("tab_chat");
  setRunning(false);
})();
