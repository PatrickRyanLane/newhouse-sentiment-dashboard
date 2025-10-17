# Editable Risk Pills Feature

This feature allows users to manually override the automatic risk level calculations in both the Brand and CEO dashboards by clicking on the risk "pills" and selecting a new value.

## 📁 Files Included

1. **editable-pills.js** - Main JavaScript module that handles the interactive functionality
2. **editable-pills.css** - Styling for the editable pills and editor interface
3. **saving-indicator.html** - HTML snippet for the save notification
4. **QUICK_INTEGRATION_GUIDE.md** - Step-by-step integration instructions
5. **EDITABLE_PILLS_GUIDE.md** - Comprehensive documentation with explanations

## 🎯 What It Does

- Makes risk level pills **clickable**
- Shows a **dropdown menu** when clicked
- Allows selection of: **Low**, **Medium**, **High**, or **Auto** (reset to automatic)
- **Saves changes** to Google Sheets via proxy servers
- **Visual indicators** show when a risk level has been manually overridden
- Changes **persist** across page refreshes

## 🚀 Quick Start

1. Read `QUICK_INTEGRATION_GUIDE.md` for detailed steps
2. Add the CSS and JavaScript files to your dashboards
3. Update your Google Apps Script proxies
4. Create a "Risk Overrides" sheet in Google Sheets
5. Test by clicking any risk pill!

## 📖 For Beginners

### What is a "pill"?
The colored badges showing "High", "Medium", or "Low" in the Risk column of your dashboards.

### Why make them editable?
Sometimes the automatic calculation might not capture the full context. This allows PR professionals to manually override the risk assessment based on their expertise.

### How does it work?
```
You click pill → Dropdown appears → Select new value → 
Saved to Google Sheets → Dashboard refreshes → Pill shows new value
```

### What's a "manual override"?
When you change a risk level yourself, that's a manual override. The pill gets a blue border and a small pencil icon (✎) to show it's been manually set.

### Can I undo it?
Yes! Just click the pill again and select "Auto (reset)" to go back to the automatic calculation.

## 🔧 Technical Details

### How It Works

1. **Module Pattern**: The code uses an IIFE (Immediately Invoked Function Expression) to create a self-contained module
2. **Event Handling**: Click handlers are attached to pills to show the editor
3. **API Integration**: Communicates with Google Sheets via Apps Script web app proxies
4. **Local Caching**: Stores overrides in memory for instant UI updates
5. **Persistence**: Reads/writes to a dedicated "Risk Overrides" sheet

### Data Flow

```
Dashboard loads 
  → Calls EditablePills.init()
    → Loads existing overrides from Google Sheets
    → Stores in Map data structure
  → Renders table
    → For each row, checks if manual override exists
    → Displays override value or auto value
  → User clicks pill
    → Shows editor dropdown
    → User selects new value
    → POST request to Apps Script proxy
    → Updates Google Sheet
    → Refreshes dashboard
```

### Google Sheets Structure

The "Risk Overrides" sheet has this structure:

| Date       | Entity      | Risk   | Timestamp           |
|------------|-------------|--------|---------------------|
| 2025-01-15 | MBTA        | High   | 2025-01-15 10:30:00 |
| 2025-01-15 | John Smith  | Low    | 2025-01-15 10:31:00 |

- **Date**: The date of the data point
- **Entity**: Company name (brand dashboard) or CEO name (CEO dashboard)
- **Risk**: The manual override value
- **Timestamp**: When the override was created/updated

### Key Components

1. **EditablePills.init()** - Loads existing overrides
2. **EditablePills.renderPill()** - Generates HTML for pills
3. **showRiskEditor()** - Shows the dropdown editor
4. **saveRiskOverride()** - Saves changes to Google Sheets
5. **getManualRisk()** - Checks if an override exists

## 🎨 Visual Design

### Normal Pill
```
┌─────────┐
│  Medium │
└─────────┘
```

### Manual Override Pill
```
╔═════════╗
║ Medium ✎║
╚═════════╝
(Blue border + pencil icon)
```

### Editor Dropdown
```
       ▲
   ┌───────┐
   │ Low   │ <- with green swatch
   │ Medium│ <- with gray swatch
   │ High  │ <- with red swatch
   ├───────┤
   │ Auto  │ <- with striped swatch
   └───────┘
```

## 🔐 Security Notes

- Apps Script must be deployed with "Anyone" access for the proxy to work
- No sensitive data is exposed (risk levels are not confidential)
- XSS protection via HTML escaping in renderPill()
- CORS handled via 'no-cors' mode (standard for Apps Script)

## 🧪 Testing Checklist

- [ ] Pills are clickable (cursor changes to pointer)
- [ ] Dropdown appears below pill when clicked
- [ ] Current value is highlighted in dropdown
- [ ] Selecting new value shows saving indicator
- [ ] Success message appears after save
- [ ] Pill updates with new value
- [ ] Manual pills show blue border + pencil
- [ ] Page refresh preserves overrides
- [ ] Google Sheet "Risk Overrides" updates
- [ ] "Auto" option removes override
- [ ] Multiple dashboards work independently

## 📊 Example Use Cases

1. **Brand Dashboard**
   - MBTA has negative SERP but it's controlled content
   - Override from "High" to "Medium"

2. **CEO Dashboard**
   - CEO has negative news but it's old/resolved
   - Override from "High" to "Low"

3. **Temporary Override**
   - Set risk to "Low" during crisis management
   - Reset to "Auto" after situation resolves

## 🤝 Contributing

To improve this feature:
1. Create a feature branch
2. Make your changes
3. Test thoroughly on both dashboards
4. Update documentation
5. Submit a pull request

## 📄 License

Part of the Newhouse Sentiment Dashboard project for Syracuse University.

## 👥 Support

Questions? Check:
1. QUICK_INTEGRATION_GUIDE.md - for setup help
2. EDITABLE_PILLS_GUIDE.md - for detailed explanations
3. Comments in the code - for technical details
4. GitHub Issues - to report problems

## 🎓 Learning Resources

**For beginners learning to code:**

- **JavaScript Modules**: [MDN Web Docs](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Modules)
- **Event Handling**: How click events work
- **Fetch API**: Making HTTP requests
- **DOM Manipulation**: Creating and modifying HTML elements
- **CSS Positioning**: How the dropdown is positioned
- **Maps**: JavaScript's key-value data structure

## 📝 Changelog

### Version 1.0 (2025-01-17)
- Initial release
- Support for Brand and CEO dashboards
- Google Sheets integration
- Visual indicators for manual overrides
- Auto-reset functionality