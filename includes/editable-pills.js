/**
 * Editable Risk Pills Module
 * 
 * This module adds the ability to click and edit risk level pills in the dashboards.
 * 
 * USAGE:
 * 1. Include this script in your HTML after the main dashboard code
 * 2. Set PROXY_URL to your Google Apps Script web app URL
 * 3. Call EditablePills.init() after your dashboard loads
 * 
 * EXPLANATION FOR BEGINNERS:
 * - This code creates a "module" (a self-contained piece of functionality)
 * - It watches for clicks on risk pills
 * - When clicked, it shows a dropdown to change the risk level
 * - It saves changes to Google Sheets via the proxy server
 * - It provides visual feedback during the process
 */

const EditablePills = (function() {
  // ============================================================================
  // CONFIGURATION
  // ============================================================================
  
  // These are the proxy server URLs you provided
  const BRAND_PROXY_URL = 'https://script.google.com/macros/s/AKfycbwMwSmXj-vE8r7hhbVaqVLFOYOWMW5cUwPQIXRTCyCF83VJ320NEcIukgbmZmUT-U4MOA/exec';
  const CEO_PROXY_URL = 'https://script.google.com/macros/s/AKfycbzvkJJ_-iYXZpMqnSt7jwRmxab7NG4XXrq08CSdyw5OHPHGE2L6pPmf-U5dB-gxTODl/exec';
  
  // Automatically choose the right proxy based on which dashboard we're on
  const PROXY_URL = window.location.pathname.includes('ceo') ? CEO_PROXY_URL : BRAND_PROXY_URL;
  
  // This Map stores any manual overrides the user has made
  // Key format: "date|entityName" -> value: "High|Medium|Low"
  let manualRiskOverrides = new Map();
  
  // Keep track of which editor is currently open (to close it when clicking elsewhere)
  let currentEditor = null;
  
  // ============================================================================
  // SAVING INDICATOR FUNCTIONS
  // ============================================================================
  
  /**
   * Shows a notification at the bottom-right of the screen
   * @param {string} text - The message to show (e.g., "Saving...")
   * @param {number} duration - How long to show it (milliseconds), or null to keep showing
   */
  function showSavingIndicator(text = 'Saving changes...', duration = null) {
    const indicator = document.getElementById('savingIndicator');
    const textEl = document.getElementById('savingText');
    
    if (!indicator || !textEl) {
      console.warn('Saving indicator elements not found. Did you add the HTML?');
      return;
    }
    
    textEl.textContent = text;
    indicator.classList.add('show');
    
    // If a duration is specified, auto-hide after that time
    if (duration) {
      setTimeout(() => indicator.classList.remove('show'), duration);
    }
    
    return indicator;
  }
  
  /**
   * Hides the saving indicator
   */
  function hideSavingIndicator() {
    const indicator = document.getElementById('savingIndicator');
    if (indicator) {
      indicator.classList.remove('show');
    }
  }
  
  // ============================================================================
  // GOOGLE SHEETS INTEGRATION
  // ============================================================================
  
  /**
   * Saves a risk level override to Google Sheets via the proxy server
   * 
   * HOW IT WORKS:
   * 1. Sends a POST request to the Google Apps Script proxy
   * 2. The proxy writes the override to a "Risk Overrides" sheet
   * 3. Updates our local cache so the UI updates immediately
   * 4. Triggers a dashboard refresh to show the new value
   * 
   * @param {string} date - The date (e.g., "2025-01-15")
   * @param {string} entity - The company or CEO name
   * @param {string} newRisk - The new risk level: "High", "Medium", "Low", or "Auto" to reset
   */
  async function saveRiskOverride(date, entity, newRisk) {
    showSavingIndicator('Saving risk level...');
    
    try {
      // Prepare the data to send to the proxy
      const payload = {
        action: 'updateRisk',
        date: date,
        entity: entity,
        risk: newRisk === 'Auto' ? '' : newRisk  // Empty string means "remove the override"
      };
      
      console.log('Saving risk override:', payload);
      
      // Send the request to Google Sheets
      // NOTE: We use 'no-cors' mode because Google Apps Script requires it
      // This means we can't read the response, but that's okay - we assume success
      const response = await fetch(PROXY_URL, {
        method: 'POST',
        mode: 'no-cors',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload)
      });
      
      // Update our local cache
      const cacheKey = `${date}|${entity}`;
      if (newRisk === 'Auto') {
        // User wants to reset to automatic calculation
        manualRiskOverrides.delete(cacheKey);
      } else {
        // User set a manual override
        manualRiskOverrides.set(cacheKey, newRisk);
      }
      
      // Show success message
      showSavingIndicator('✓ Saved successfully!', 2000);
      
      // Refresh the dashboard after a brief delay
      // This gives time for the success message to show
      setTimeout(() => {
        if (typeof renderAll === 'function') {
          renderAll(); // Call the dashboard's render function
        } else {
          console.warn('renderAll() function not found - manual refresh may be needed');
        }
      }, 500);
      
      return true;
    } catch (error) {
      console.error('Error saving risk override:', error);
      showSavingIndicator('✗ Error saving. Please try again.', 3000);
      return false;
    }
  }
  
  /**
   * Loads existing risk overrides from Google Sheets
   * 
   * This is called when the dashboard first loads to restore any
   * manual overrides the user made previously.
   */
  async function loadRiskOverrides() {
    try {
      // Request the list of overrides from the proxy
      const response = await fetch(`${PROXY_URL}?action=getRiskOverrides`, {
        method: 'GET',
        headers: {
          'Accept': 'application/json'
        }
      });
      
      const data = await response.json();
      console.log('Loaded risk overrides:', data);
      
      // Populate our cache
      if (data && data.success && data.overrides) {
        manualRiskOverrides = new Map(
          data.overrides.map(o => [`${o.date}|${o.entity}`, o.risk])
        );
      }
    } catch (error) {
      console.warn('Could not load risk overrides:', error);
      // This is not critical - we can continue without overrides
      // The user can still create new ones
    }
  }
  
  /**
   * Checks if a specific entity has a manual risk override for a given date
   * @returns {string|null} The manual risk level, or null if using auto calculation
   */
  function getManualRisk(date, entity) {
    return manualRiskOverrides.get(`${date}|${entity}`);
  }
  
  // ============================================================================
  // RISK EDITOR UI
  // ============================================================================
  
  /**
   * Creates and displays the risk editor dropdown below a pill
   * 
   * HOW IT WORKS:
   * 1. Creates a dropdown div with risk level options
   * 2. Positions it below the clicked pill
   * 3. Highlights the current risk level
   * 4. Sets up click handlers for each option
   * 5. Closes the dropdown when user clicks outside it
   * 
   * @param {HTMLElement} pillElement - The pill that was clicked
   * @param {string} currentRisk - The current risk level
   * @param {string} date - The date for this row
   * @param {string} entity - The company/CEO name for this row
   */
  function showRiskEditor(pillElement, currentRisk, date, entity) {
    // Close any existing editor first
    closeRiskEditor();
    
    // Create the editor div
    const editor = document.createElement('div');
    editor.className = 'risk-editor';
    
    // Build the options HTML
    // We use a template literal (backticks) to make multi-line strings easier
    editor.innerHTML = `
      <div class="risk-option ${currentRisk === 'Low' ? 'selected' : ''}" data-risk="Low">
        <div class="risk-swatch low"></div>
        <span>Low</span>
      </div>
      <div class="risk-option ${currentRisk === 'Medium' ? 'selected' : ''}" data-risk="Medium">
        <div class="risk-swatch med"></div>
        <span>Medium</span>
      </div>
      <div class="risk-option ${currentRisk === 'High' ? 'selected' : ''}" data-risk="High">
        <div class="risk-swatch high"></div>
        <span>High</span>
      </div>
      <div style="height:1px;background:rgba(255,255,255,0.1);margin:4px 0"></div>
      <div class="risk-option" data-risk="Auto">
        <div class="risk-swatch auto"></div>
        <span>Auto (reset)</span>
      </div>
    `;
    
    // Find the table cell containing this pill and make it position: relative
    // This allows us to position the editor relative to the cell
    const pillParent = pillElement.closest('td');
    const originalPosition = pillParent.style.position;
    pillParent.style.position = 'relative';
    
    // Add the editor to the DOM
    pillParent.appendChild(editor);
    currentEditor = { element: editor, originalPosition, pillParent };
    
    // Set up click handlers for each risk option
    editor.querySelectorAll('.risk-option').forEach(option => {
      option.addEventListener('click', async (e) => {
        e.stopPropagation(); // Don't trigger other click handlers
        
        const newRisk = option.getAttribute('data-risk');
        console.log(`User selected: ${newRisk} for ${entity} on ${date}`);
        
        // Save the override (this will also refresh the table)
        await saveRiskOverride(date, entity, newRisk);
        
        // Close the editor
        closeRiskEditor();
      });
    });
    
    // Set up click-outside-to-close behavior
    // We use setTimeout to avoid immediately closing when the pill is clicked
    setTimeout(() => {
      document.addEventListener('click', outsideClickHandler);
    }, 0);
  }
  
  /**
   * Closes the currently open risk editor
   */
  function closeRiskEditor() {
    if (currentEditor) {
      // Remove the editor element
      if (currentEditor.element && currentEditor.element.parentNode) {
        currentEditor.element.remove();
      }
      
      // Restore the original position style
      if (currentEditor.pillParent && currentEditor.originalPosition !== undefined) {
        currentEditor.pillParent.style.position = currentEditor.originalPosition;
      }
      
      // Remove the outside click handler
      document.removeEventListener('click', outsideClickHandler);
      
      currentEditor = null;
    }
  }
  
  /**
   * Handles clicks outside the editor to close it
   */
  function outsideClickHandler(e) {
    if (!currentEditor) return;
    
    const editor = currentEditor.element;
    const clickedInsideEditor = editor && editor.contains(e.target);
    const clickedPill = e.target.classList && e.target.classList.contains('pill');
    
    // Close if clicked outside both the editor and any pill
    if (!clickedInsideEditor && !clickedPill) {
      closeRiskEditor();
    }
  }
  
  // ============================================================================
  // PILL RENDERING HELPER
  // ============================================================================
  
  /**
   * Generates the HTML for a risk pill with edit functionality
   * 
   * This function is meant to be called from your dashboard's renderTable() function
   * to replace the existing pill rendering code.
   * 
   * EXAMPLE USAGE in renderTable():
   *   <td class="center">${EditablePills.renderPill(r.date, r.company, r.risk)}</td>
   * 
   * @param {string} date - The date
   * @param {string} entity - The company or CEO name
   * @param {string} autoRisk - The automatically calculated risk level
   * @returns {string} HTML string for the pill
   */
  function renderPill(date, entity, autoRisk) {
    // Check if there's a manual override
    const manualRisk = getManualRisk(date, entity);
    const displayRisk = manualRisk || autoRisk;
    const isManual = !!manualRisk;
    
    // Escape function to prevent XSS attacks
    const esc = (s) => String(s ?? '').replace(/[&<>"']/g, m => ({
      '&': '&amp;',
      '<': '&lt;',
      '>': '&gt;',
      '"': '&quot;',
      "'": '&#39;'
    }[m]));
    
    // Handle valid risk levels
    if (displayRisk === 'High' || displayRisk === 'Medium' || displayRisk === 'Low') {
      // Determine the CSS class
      const pillClass = displayRisk === 'High' ? 'pill high' 
                      : displayRisk === 'Medium' ? 'pill med' 
                      : 'pill low';
      
      // Add 'manual' class if this is an override
      const manualClass = isManual ? ' manual' : '';
      
      // Determine the tooltip
      const title = isManual 
        ? 'Manual override - click to edit' 
        : 'Click to override automatic risk calculation';
      
      // Return the HTML
      // We use onclick to call our showRiskEditor function
      return `<span class="${pillClass}${manualClass}" 
                    data-date="${esc(date)}" 
                    data-entity="${esc(entity)}"
                    data-risk="${esc(displayRisk)}"
                    title="${title}"
                    style="cursor:pointer"
                    onclick="EditablePills.showRiskEditor(this, '${esc(displayRisk)}', '${esc(date)}', '${esc(entity)}')">${displayRisk}</span>`;
    }
    
    // If risk is N/A or invalid, show muted text
    return '<span class="muted">N/A</span>';
  }
  
  // ============================================================================
  // INITIALIZATION
  // ============================================================================
  
  /**
   * Initializes the editable pills system
   * 
   * Call this after your dashboard has loaded its data and rendered the initial table.
   * 
   * EXAMPLE:
   *   async function init() {
   *     await Promise.all([loadData1(), loadData2()]);
   *     renderAll();
   *     await EditablePills.init();  // Add this line
   *   }
   */
  async function init() {
    console.log('Initializing EditablePills module...');
    console.log('Using proxy:', PROXY_URL);
    
    // Load any existing overrides from Google Sheets
    await loadRiskOverrides();
    
    console.log('EditablePills initialized. Loaded', manualRiskOverrides.size, 'overrides.');
    
    // If the dashboard has already rendered, we might need to re-render
    // to apply the manual overrides
    if (typeof renderAll === 'function' && manualRiskOverrides.size > 0) {
      console.log('Re-rendering to apply overrides...');
      renderAll();
    }
  }
  
  // ============================================================================
  // PUBLIC API
  // ============================================================================
  
  // Return the public interface of this module
  // These are the functions that can be called from outside this file
  return {
    init: init,
    showRiskEditor: showRiskEditor,
    renderPill: renderPill,
    getManualRisk: getManualRisk,
    saveRiskOverride: saveRiskOverride
  };
})();

// For debugging in the browser console
window.EditablePills = EditablePills;