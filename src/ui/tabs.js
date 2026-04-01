'use strict';
import { appState } from '../../state.js';
import { logger } from '../../logger.js';

// Switch between tabs
export function switchTab(tabName) {
  // ── 1. Visual switch — fast, runs inside the click task ──────────────────
  // These DOM updates paint immediately, giving instant feedback to the user
  // and completing the INP measurement before any heavy work begins.
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.tab === tabName);
  });
  document.querySelectorAll('.tab-content').forEach(content => {
    content.classList.toggle('active', content.id === `${tabName}-tab`);
  });

  // ── 2. Heavy work — deferred to a new task ────────────────────────────────
  // populateChartFilters() + renderChart() (Chart.js canvas draw) are
  // synchronous and expensive on large datasets.  Running them inside the
  // click handler causes 800 ms+ INP violations.  setTimeout(fn, 0) ends the
  // current task so the browser can paint the tab switch first, then runs the
  // analytics init as a separate, lower-priority task.
  if (tabName === 'analytics') {
    const analyticsTab = document.getElementById('analytics-tab');
    if (analyticsTab) {
      // Show a lightweight placeholder so the tab doesn't look blank
      // while the chart is initializing in the deferred task.
      const existing = analyticsTab.querySelector('#chart-loading-placeholder');
      if (!existing) {
        const placeholder = document.createElement('div');
        placeholder.id = 'chart-loading-placeholder';
        placeholder.style.cssText =
          'display:flex;align-items:center;justify-content:center;height:200px;color:#94a3b8;font-size:14px;';
        placeholder.textContent = 'Loading chart…';
        const container = analyticsTab.querySelector('#chartContainer');
        if (container) container.appendChild(placeholder);
      }
    }

    setTimeout(() => {
      const placeholder = document.getElementById('chart-loading-placeholder');
      if (placeholder) placeholder.remove();

      if (typeof appState.onInitializeAnalyticsTab === 'function') {
        appState.onInitializeAnalyticsTab();
      }
    }, 0);
  }
}

// Setup tab event listeners
export function setupTabEventListeners() {
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      switchTab(btn.dataset.tab);
    });
  });
}
