/**
 * Vercel Serverless Function - Google Sheets Proxy
 * 
 * This replaces the Apps Script proxy with a more reliable solution.
 * Handles READ and UPDATE_SENTIMENT actions for both brand and CEO sheets.
 * 
 * Environment Variables Required (set in Vercel dashboard):
 * - GOOGLE_CREDENTIALS: Base64-encoded service account JSON
 * - GOOGLE_SHEET_ID_BRAND: Your brand Google Sheet ID
 * - GOOGLE_SHEET_ID_CEO: Your CEO Google Sheet ID
 */

const { google } = require('googleapis');

// Configuration - Using same names as GitHub Secrets for consistency
const BRAND_SHEET_ID = process.env.GOOGLE_SHEET_ID_BRAND || '15x5AYC3igVZ0AnWavcZpPA8ESSWVF9msi5vztuaqCTw';
const CEO_SHEET_ID = process.env.GOOGLE_SHEET_ID_CEO || '1RGAgs7aWs_LkqOZN2cDOM06vLAhlJa4Ck_bkgkJ9Gbs';
const ALLOWED_SENTIMENTS = ['positive', 'neutral', 'negative'];

/**
 * Get authenticated Google Sheets client
 */
function getGoogleSheetsClient() {
  // Decode credentials from environment variable
  const credentialsBase64 = process.env.GOOGLE_CREDENTIALS;
  
  if (!credentialsBase64) {
    throw new Error('GOOGLE_CREDENTIALS environment variable not set');
  }
  
  const credentials = JSON.parse(
    Buffer.from(credentialsBase64, 'base64').toString('utf-8')
  );
  
  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  
  return google.sheets({ version: 'v4', auth });
}

/**
 * Determine which sheet ID to use based on sheet name
 */
function getSheetId(sheetName) {
  if (sheetName.toLowerCase().includes('brand')) {
    return BRAND_SHEET_ID;
  } else if (sheetName.toLowerCase().includes('ceo')) {
    return CEO_SHEET_ID;
  }
  return BRAND_SHEET_ID;
}

/**
 * Handle READ action - Get all data from a sheet
 */
async function handleRead(data) {
  const { sheetName } = data;
  
  if (!sheetName) {
    return {
      success: false,
      error: 'Missing required field: sheetName'
    };
  }
  
  try {
    const sheets = getGoogleSheetsClient();
    const sheetId = getSheetId(sheetName);
    
    // Try to read the sheet
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: sheetId,
      range: `${sheetName}!A:ZZ`,
    });
    
    const values = response.data.values || [];
    
    if (values.length === 0) {
      return {
        success: true,
        data: []
      };
    }
    
    // Convert to array of objects
    const headers = values[0];
    const rows = [];
    
    for (let i = 1; i < values.length; i++) {
      const row = {};
      for (let j = 0; j < headers.length; j++) {
        row[headers[j]] = values[i][j];
      }
      rows.push(row);
    }
    
    console.log(`✓ Read ${rows.length} rows from "${sheetName}"`);
    
    return {
      success: true,
      data: rows
    };
    
  } catch (error) {
    // If sheet doesn't exist, return empty data (not an error)
    if (error.code === 400 || error.message?.includes('Unable to parse range')) {
      console.log(`⚠️ Sheet "${sheetName}" not found, returning empty data`);
      return {
        success: true,
        data: []
      };
    }
    
    console.error(`❌ Error reading sheet: ${error.message}`);
    return {
      success: false,
      error: error.message
    };
  }
}

/**
 * Handle UPDATE_SENTIMENT action - Update a specific row
 */
async function handleUpdateSentiment(data) {
  const { sheetName, url, sentiment } = data;
  
  // Validate required fields
  if (!sheetName || !url || !sentiment) {
    return {
      success: false,
      error: 'Missing required fields: sheetName, url, sentiment'
    };
  }
  
  // Validate sentiment value
  if (!ALLOWED_SENTIMENTS.includes(sentiment)) {
    return {
      success: false,
      error: `Invalid sentiment. Must be one of: ${ALLOWED_SENTIMENTS.join(', ')}`
    };
  }
  
  try {
    const sheets = getGoogleSheetsClient();
    const sheetId = getSheetId(sheetName);
    
    // Read current data
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: sheetId,
      range: `${sheetName}!A:ZZ`,
    });
    
    const values = response.data.values || [];
    
    if (values.length < 2) {
      return {
        success: false,
        error: 'Sheet has no data rows'
      };
    }
    
    // Find column indices
    const headers = values[0];
    const urlCol = headers.indexOf('url');
    const sentimentCol = headers.indexOf('sentiment');
    
    if (urlCol === -1) {
      return {
        success: false,
        error: 'URL column not found in sheet'
      };
    }
    
    if (sentimentCol === -1) {
      return {
        success: false,
        error: 'Sentiment column not found in sheet'
      };
    }
    
    // Find the row with matching URL
    let rowIndex = -1;
    for (let i = 1; i < values.length; i++) {
      if (values[i][urlCol] === url) {
        rowIndex = i;
        break;
      }
    }
    
    if (rowIndex === -1) {
      return {
        success: false,
        error: 'URL not found in sheet. The article may not exist.'
      };
    }
    
    // Store old value
    const oldSentiment = values[rowIndex][sentimentCol];
    
    // Update the value
    values[rowIndex][sentimentCol] = sentiment;
    
    // Write back to sheet
    await sheets.spreadsheets.values.update({
      spreadsheetId: sheetId,
      range: `${sheetName}!A:ZZ`,
      valueInputOption: 'RAW',
      requestBody: {
        values: values
      }
    });
    
    console.log(`✓ Row ${rowIndex + 1}: sentiment changed from \"${oldSentiment}\" to \"${sentiment}\"`);\n    console.log(`  URL: ${url}`);\n    \n    return {
      success: true,
      message: 'Sentiment updated successfully',\n      oldValue: oldSentiment,\n      newValue: sentiment,\n      rowIndex: rowIndex + 1,\n      timestamp: new Date().toISOString()
    };
    \n  } catch (error) {\n    console.error(`❌ Error updating sentiment: ${error.message}`);\n    return {\n      success: false,\n      error: error.message\n    };\n  }\n}\n\n/**\n * Main handler - Routes requests to appropriate functions\n */\nmodule.exports = async (req, res) => {\n  // Enable CORS for all origins\n  res.setHeader('Access-Control-Allow-Origin', '*');\n  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');\n  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');\n  \n  // Handle OPTIONS preflight request\n  if (req.method === 'OPTIONS') {\n    res.status(200).end();\n    return;\n  }\n  \n  // Handle GET requests (health check)\n  if (req.method === 'GET') {\n    res.status(200).send(\n      '✓ MBTA Dashboard Proxy is running!\\n\\n' +\n      'This endpoint accepts POST requests with actions:\\n' +\n      '  • READ - Get all data from a sheet\\n' +\n      '  • UPDATE_SENTIMENT - Update sentiment for a URL\\n\\n' +\n      'Configured for:\\n' +\n      `  • Brand Sheet: ${BRAND_SHEET_ID}\\n` +\n      `  • CEO Sheet: ${CEO_SHEET_ID}\\n\\n` +\n      `Time: ${new Date().toISOString()}`\n    );\n    return;\n  }\n  \n  // Handle POST requests\n  if (req.method === 'POST') {\n    try {\n      const data = req.body;\n      \n      // Validate action\n      if (!data.action) {\n        res.status(400).json({\n          success: false,\n          error: 'Missing required field: action'\n        });\n        return;\n      }\n      \n      // Route to appropriate handler\n      let result;\n      if (data.action === 'READ') {\n        result = await handleRead(data);\n      } else if (data.action === 'UPDATE_SENTIMENT') {\n        result = await handleUpdateSentiment(data);\n      } else {\n        res.status(400).json({\n          success: false,\n          error: `Unknown action: ${data.action}`\n        });\n        return;\n      }\n      \n      res.status(200).json(result);\n      \n    } catch (error) {\n      console.error('Error processing request:', error);\n      res.status(500).json({\n        success: false,\n        error: error.message\n      });\n    }\n    return;\n  }\n  \n  // Method not allowed\n  res.status(405).json({\n    success: false,\n    error: 'Method not allowed'\n  });\n};\n