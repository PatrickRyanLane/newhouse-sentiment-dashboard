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
 * 
 * Version: 1.1.0 - Added edit tracking
 */

const { google } = require('googleapis');

// Configuration - Using same names as GitHub Secrets for consistency
const BRAND_SHEET_ID = process.env.GOOGLE_SHEET_ID_BRAND || '15x5AYC3igVZ0AnWavcZpPA8ESSWVF9msi5vztuaqCTw';
const CEO_SHEET_ID = process.env.GOOGLE_SHEET_ID_CEO || '1RGAgs7aWs_LkqOZN2cDOM06vLAhlJa4Ck_bkgkJ9Gbs';
const ALLOWED_SENTIMENTS = ['positive', 'neutral', 'negative'];
const ALLOWED_CONTROLLED = ['controlled', 'uncontrolled'];

/**
 * Parse request body (handles both JSON and raw string)
 */
async function parseBody(req) {
  if (req.body && typeof req.body === 'object') {
    return req.body;
  }
  
  if (typeof req.body === 'string') {
    return JSON.parse(req.body);
  }
  
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', chunk => body += chunk.toString());
    req.on('end', () => {
      try {
        resolve(JSON.parse(body));
      } catch (e) {
        reject(e);
      }
    });
  });
}

/**
 * Get authenticated Google Sheets client
 */
function getGoogleSheetsClient() {
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
 * Handle UPDATE_SENTIMENT action - Update sentiment and/or controlled fields
 * 
 * NEW: Tracks manual edits by setting *_edited columns to 'true'
 */
async function handleUpdateSentiment(data) {
  const { sheetName, url, sentiment, controlled, markEdited } = data;
  
  if (!sheetName || !url) {
    return {
      success: false,
      error: 'Missing required fields: sheetName, url'
    };
  }
  
  if (!sentiment && !controlled) {
    return {
      success: false,
      error: 'Must provide at least one field to update: sentiment or controlled'
    };
  }
  
  if (sentiment && !ALLOWED_SENTIMENTS.includes(sentiment)) {
    return {
      success: false,
      error: `Invalid sentiment. Must be one of: ${ALLOWED_SENTIMENTS.join(', ')}`
    };
  }
  
  if (controlled && !ALLOWED_CONTROLLED.includes(controlled)) {
    return {
      success: false,
      error: `Invalid controlled. Must be one of: ${ALLOWED_CONTROLLED.join(', ')}`
    };
  }
  
  try {
    const sheets = getGoogleSheetsClient();
    const sheetId = getSheetId(sheetName);
    
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
    
    const headers = values[0];
    const urlCol = headers.indexOf('url');
    const sentimentCol = headers.indexOf('sentiment');
    const controlledCol = headers.indexOf('controlled');
    
    // Find edit tracking columns (create if needed)
    let sentimentEditedCol = headers.indexOf('sentiment_edited');
    let controlledEditedCol = headers.indexOf('controlled_edited');
    
    // Add edit tracking columns if they don't exist
    if (sentimentEditedCol === -1 && markEdited) {
      headers.push('sentiment_edited');
      sentimentEditedCol = headers.length - 1;
    }
    if (controlledEditedCol === -1 && markEdited) {
      headers.push('controlled_edited');
      controlledEditedCol = headers.length - 1;
    }
    
    // Update header row if we added columns
    values[0] = headers;
    
    if (urlCol === -1) {
      return {
        success: false,
        error: 'URL column not found in sheet'
      };
    }
    
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
    
    // Ensure row has enough columns
    while (values[rowIndex].length < headers.length) {
      values[rowIndex].push('');
    }
    
    const changes = [];
    let updated = false;
    
    // Update sentiment if provided
    if (sentiment && sentimentCol !== -1) {
      const oldValue = values[rowIndex][sentimentCol];
      values[rowIndex][sentimentCol] = sentiment;
      changes.push(`sentiment: ${oldValue} → ${sentiment}`);
      
      // Mark as manually edited
      if (markEdited && sentimentEditedCol !== -1) {
        values[rowIndex][sentimentEditedCol] = 'true';
        changes.push('marked sentiment_edited');
      }
      updated = true;
    }
    
    // Update controlled if provided
    if (controlled && controlledCol !== -1) {
      const oldValue = values[rowIndex][controlledCol];
      values[rowIndex][controlledCol] = controlled;
      changes.push(`controlled: ${oldValue} → ${controlled}`);
      
      // Mark as manually edited
      if (markEdited && controlledEditedCol !== -1) {
        values[rowIndex][controlledEditedCol] = 'true';
        changes.push('marked controlled_edited');
      }
      updated = true;
    }
    
    if (!updated) {
      return {
        success: false,
        error: 'No valid fields to update'
      };
    }
    
    // Write back to sheet
    await sheets.spreadsheets.values.update({
      spreadsheetId: sheetId,
      range: `${sheetName}!A:ZZ`,
      valueInputOption: 'RAW',
      requestBody: {
        values: values
      }
    });
    
    console.log(`✓ Row ${rowIndex + 1}: ${changes.join(', ')}`);
    console.log(`  URL: ${url}`);
    
    return {
      success: true,
      message: 'Successfully updated',
      changes: changes,
      rowIndex: rowIndex + 1,
      timestamp: new Date().toISOString()
    };
    
  } catch (error) {
    console.error(`❌ Error updating: ${error.message}`);
    return {
      success: false,
      error: error.message
    };
  }
}

/**
 * Main handler - Routes requests to appropriate functions
 */
module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  
  if (req.method === 'OPTIONS') {
    res.status(200).end();
    return;
  }
  
  if (req.method === 'GET') {
    res.status(200).send(
      '✓ MBTA Dashboard Proxy is running!\n\n' +
      'This endpoint accepts POST requests with actions:\n' +
      '  • READ - Get all data from a sheet\n' +
      '  • UPDATE_SENTIMENT - Update sentiment and/or controlled (with edit tracking)\n\n' +
      'Configured for:\n' +
      `  • Brand Sheet: ${BRAND_SHEET_ID}\n` +
      `  • CEO Sheet: ${CEO_SHEET_ID}\n\n` +
      `Time: ${new Date().toISOString()}`
    );
    return;
  }
  
  if (req.method === 'POST') {
    try {
      const data = await parseBody(req);
      
      console.log('Received request:', JSON.stringify(data));
      
      if (!data || !data.action) {
        res.status(400).json({
          success: false,
          error: 'Missing required field: action',
          received: data
        });
        return;
      }
      
      let result;
      if (data.action === 'READ') {
        result = await handleRead(data);
      } else if (data.action === 'UPDATE_SENTIMENT') {
        result = await handleUpdateSentiment(data);
      } else {
        res.status(400).json({
          success: false,
          error: `Unknown action: ${data.action}`
        });
        return;
      }
      
      res.status(200).json(result);
      
    } catch (error) {
      console.error('Error processing request:', error);
      res.status(500).json({
        success: false,
        error: error.message,
        stack: error.stack
      });
    }
    return;
  }
  
  res.status(405).json({
    success: false,
    error: 'Method not allowed'
  });
};
