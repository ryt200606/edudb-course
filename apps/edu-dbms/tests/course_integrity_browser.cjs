'use strict';
const fs = require('node:fs');
const path = require('node:path');
const { chromium } = require('playwright');

async function main() {
  const [url, output, fixturePath] = process.argv.slice(2);
  const cases = JSON.parse(fs.readFileSync(fixturePath, 'utf8'));
  const browser = await chromium.launch({
    headless: true,
    ...(process.env.EDUDB_BROWSER_EXECUTABLE ? {executablePath: process.env.EDUDB_BROWSER_EXECUTABLE} : {})
  });
  const records = [];
  const errors = [];
  try {
    const page = await browser.newPage({viewport: {width: 1440, height: 960}});
    page.on('pageerror', error => errors.push(String(error)));
    await page.goto(url);
    await page.locator('#execute:enabled').waitFor();
    for (const item of cases) {
      await page.locator('#sql').fill(item.sql);
      const response = page.waitForResponse(r => r.url().endsWith('/sql') && r.request().method() === 'POST');
      await page.locator('#execute').click();
      const body = await (await response).json();
      await page.locator('#execute:enabled').waitFor();
      const record = {case: item.case, ok: body.ok, rows: body.rows, error: body.error, transaction_state: body.transaction_state};
      if (item.expected_rows) {
        record.rendered = await page.locator('#result-table tbody tr').evaluateAll(rows =>
          rows.map(row => Array.from(row.querySelectorAll('td'), cell => cell.textContent)));
      }
      if (item.expected_error)
        record.message = await page.locator('#message').innerText();
      records.push(record);
    }
    await page.screenshot({path: path.join(path.dirname(output), 'selected-rows.png')});
    await page.locator('#sql').fill('DESCRIBE ci_enrol');
    const schemaResponse=page.waitForResponse(r=>r.url().endsWith('/sql')&&r.request().method()==='POST');
    await page.locator('#execute').click();
    const schema=await (await schemaResponse).json();
    const terms=page.locator('section.table-constraints dt');
    await terms.first().waitFor();
    const shown=await terms.allTextContents();
    const definitions=await page.locator('section.table-constraints dd').allTextContents();
    if(!schema.ok||shown.length!==3||!shown.includes('ci_score')||!definitions.some(d=>d.includes('ON DELETE CASCADE'))||!definitions.some(d=>d.includes('ON DELETE RESTRICT')))
      throw Error('DESCRIBE did not display all named constraints: '+JSON.stringify({shown,definitions}));
    fs.writeFileSync(path.join(path.dirname(output),'schema-constraints.json'),JSON.stringify({shown,definitions},null,2));
    await page.screenshot({path:path.join(path.dirname(output),'schema-constraints.png')});

    if (errors.length) throw new Error(errors.join('\n'));
    fs.writeFileSync(output, JSON.stringify(records, null, 2));
  } finally {
    await browser.close();
  }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
