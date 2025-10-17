# Quick Integration Guide for Editable Risk Pills

## 📋 What You Need
1. `editable-pills.css` - The stylesheet
2. `editable-pills.js` - The JavaScript module
3. `saving-indicator.html` - HTML for the save notification
4. Updates to your Google Apps Script proxy

## 🚀 Integration Steps

### Step 1: Add the Files to Your Repository

The files are already in your repository under the `includes` directory:
- `includes/editable-pills.css`
- `includes/editable-pills.js`  

### Step 2: Update `brand-dashboard.html`

#### A. Add CSS (in the `<head>` section, after Chart.js)

```html
<!-- Editable Pills CSS -->
<link rel="stylesheet" href="includes/editable-pills.css">
```

OR copy the contents of `editable-pills.css` into your existing `<style>` tag.

#### B. Add JavaScript (before the closing `</body>` tag)

```html
<!-- Editable Pills Module -->
<script src="includes/editable-pills.js"></script>
```

#### C. Add Saving Indicator HTML (before closing `</body>` tag, after the modal)

```html
<!-- Saving indicator -->
<div id="savingIndicator" class="saving-indicator">
  <div class="spinner"></div>
  <span id="savingText">Saving changes...</span>
</div>
```

#### D. Update the `init()` function

Find your existing `init()` function and add ONE LINE at the end:

```javascript
async function init(){
  await Promise.all([loadCounts(), loadSerpDaily()]);
  // ... rest of your existing init code ...
  
  await EditablePills.init();  // <-- ADD THIS LINE
}
```

#### E. Update `renderTable()` to use editable pills

Find this section in `renderTable()`:

```javascript
<td class=\"center\">${
  (r.risk==='High'||r.risk==='Medium'||r.risk==='Low')
    ? (r.risk==='High'
        ? '<span class=\"pill high\">High</span>'
        : r.risk==='Medium'
          ? '<span class=\"pill med\">Medium</span>'
          : '<span class=\"pill low\">Low</span>')
    : '<span class=\"muted\">N/A</span>'
}</td>
```

Replace it with:

```javascript
<td class=\"center\">${EditablePills.renderPill(r.date, r.company, r.risk)}</td>
```

**That's it for the brand dashboard!**

### Step 3: Update `ceo-dashboard.html`

Repeat the EXACT same steps as for brand-dashboard.html, with ONE difference:

In step E, when updating `renderTable()`, use:
```javascript
<td class=\"center\">${EditablePills.renderPill(r.date, r.ceo, r.risk)}</td>
```

Note: `r.ceo` instead of `r.company` since CEO dashboard uses CEO names.

### Step 4: Update Your Google Apps Script

#### A. Create "Risk Overrides" Sheet

In your Google Sheet, create a new sheet named exactly: **Risk Overrides**

Add these column headers in row 1:
- **A1**: Date
- **B1**: Entity
- **C1**: Risk
- **D1**: Timestamp

#### B. Update Brand Dashboard Apps Script

Replace your entire Apps Script code with:

```javascript
// Brand Dashboard Apps Script Proxy
const SHEET_ID = 'YOUR_SHEET_ID_HERE';  // <-- UPDATE THIS

function doGet(e) {
  if (e.parameter.action === 'getRiskOverrides') {
    return getRiskOverrides();
  }
  return ContentService.createTextOutput(JSON.stringify({
    error: 'Invalid action'
  })).setMimeType(ContentService.MimeType.JSON);
}

function doPost(e) {
  try {
    const data = JSON.parse(e.postData.contents);
    
    if (data.action === 'updateRisk') {
      return updateRiskLevel(data);
    }
    
    return ContentService.createTextOutput(JSON.stringify({
      success: false,
      error: 'Unknown action'
    })).setMimeType(ContentService.MimeType.JSON);
    
  } catch (error) {
    return ContentService.createTextOutput(JSON.stringify({
      success: false,
      error: error.toString()
    })).setMimeType(ContentService.MimeType.JSON);
  }
}

function updateRiskLevel(data) {
  const ss = SpreadsheetApp.openById(SHEET_ID);
  let sheet = ss.getSheetByName('Risk Overrides');
  
  // Create the sheet if it doesn't exist
  if (!sheet) {
    sheet = ss.insertSheet('Risk Overrides');
    sheet.appendRow(['Date', 'Entity', 'Risk', 'Timestamp']);
  }
  
  const { date, entity, risk } = data;
  
  // Find existing override
  const dataRange = sheet.getDataRange();
  const values = dataRange.getValues();
  
  let rowIndex = -1;
  for (let i = 1; i < values.length; i++) {
    if (values[i][0] === date && values[i][1] === entity) {
      rowIndex = i + 1;
      break;
    }
  }
  
  if (risk === '') {
    // Delete the override
    if (rowIndex > 0) {
      sheet.deleteRow(rowIndex);
    }
  } else {
    // Add or update override
    const timestamp = new Date();
    if (rowIndex > 0) {
      sheet.getRange(rowIndex, 3, 1, 2).setValues([[risk, timestamp]]);
    } else {
      sheet.appendRow([date, entity, risk, timestamp]);
    }
  }
  
  return ContentService.createTextOutput(JSON.stringify({
    success: true,
    message: 'Risk level updated'
  })).setMimeType(ContentService.MimeType.JSON);
}

function getRiskOverrides() {
  const ss = SpreadsheetApp.openById(SHEET_ID);
  const sheet = ss.getSheetByName('Risk Overrides');
  
  if (!sheet) {
    return ContentService.createTextOutput(JSON.stringify({
      success: true,
      overrides: []
    })).setMimeType(ContentService.MimeType.JSON);
  }
  
  const values = sheet.getDataRange().getValues();
  const overrides = [];
  
  for (let i = 1; i < values.length; i++) {
    if (values[i][0] && values[i][1] && values[i][2]) {
      overrides.push({
        date: values[i][0].toString(),
        entity: values[i][1].toString(),
        risk: values[i][2].toString()
      });
    }
  }
  
  return ContentService.createTextOutput(JSON.stringify({
    success: true,
    overrides: overrides
  })).setMimeType(ContentService.MimeType.JSON);
}
```

#### C. Update CEO Dashboard Apps Script

Use the **same code** as above, just change the SHEET_ID to your CEO dashboard sheet.

#### D. Deploy as Web Apps

For BOTH scripts:
1. Click **Deploy** > **New deployment**
2. Select type: **Web app**
3. Execute as: **Me**
4. Who has access: **Anyone**
5. Click **Deploy**
6. Copy the web app URL
7. Verify it matches the URLs in your dashboards

## ✅ Testing

1. Open your dashboard
2. Click on any risk pill
3. A dropdown should appear with options: Low, Medium, High, Auto
4. Select a different risk level
5. You should see "Saving..." then "✓ Saved successfully!" in bottom-right
6. The pill should update with a blue border and pencil icon (✎)
7. Refresh the page - the override should persist
8. Check your Google Sheet - the override should be in the "Risk Overrides" sheet

## 🎨 Visual Indicators

- **Normal pill**: Auto-calculated risk
- **Pill with blue border + ✎**: Manual override
- **Hover**: Pill scales slightly
- **Dropdown**: Click pill to edit
- **Saving notification**: Bottom-right corner shows status

## 🐛 Troubleshooting

### Pills not clickable?
- Check browser console for errors (F12)
- Verify `editable-pills.js` loaded successfully
- Make sure you added `await EditablePills.init()` to your `init()` function

### Dropdown not appearing?
- Check that `editable-pills.css` loaded
- Verify there are no JavaScript errors
- Make sure you're clicking directly on the pill text

### Changes not saving?
- Check Apps Script execution logs (View > Executions in Apps Script editor)
- Verify your web app is deployed with "Anyone" access
- Check browser console (F12) for network errors
- Verify SHEET_ID is correct in Apps Script
- Make sure "Risk Overrides" sheet exists

### Overrides not loading on refresh?
- Check that `getRiskOverrides()` returns data
- Verify Apps Script GET requests work (try the URL directly in browser)
- Check browser console for CORS or network errors

### Wrong dashboard being affected?
- The script auto-detects which dashboard based on URL
- Brand dashboard = uses company names
- CEO dashboard = uses CEO names
- Make sure your data has the right field names

## 📝 Important Notes

1. **Data Persistence**: Overrides are stored in Google Sheets, not in the CSV files. This means:
   - CSV files remain unchanged (good for version control)
   - Overrides persist across data refreshes
   - Overrides are separate from the main data pipeline

2. **Performance**: The system loads all overrides on dashboard init. If you have thousands of overrides, you might want to add pagination or filtering.

3. **Permissions**: Make sure your Apps Script web app is deployed with "Anyone" access, otherwise the fetch requests will fail.

4. **Browser Compatibility**: Tested on modern Chrome, Firefox, Safari. May need polyfills for IE11.

## 🎯 Summary

You've added:
- ✓ Clickable risk pills
- ✓ Dropdown editor for changing risk levels
- ✓ Google Sheets integration for persistence
- ✓ Visual feedback during saves
- ✓ Manual override indicators (blue border + ✎)
- ✓ Ability to reset to automatic calculation

Users can now override risk calculations with a simple click!