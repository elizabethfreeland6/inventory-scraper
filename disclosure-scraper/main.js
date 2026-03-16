import { Actor } from 'apify';
import * as cheerio from 'cheerio';
import OpenAI from 'openai';

// ─────────────────────────────────────────────────────────────────────────────
// DEALER DISCLOSURE SCRAPER
// Visits a sample of vehicle detail pages for each competitor dealer,
// extracts all disclosure/fine print text, and uses GPT to parse out:
//   - Documentation fee
//   - Market adjustments / dealer add-ons
//   - Financing rates disclosed
//   - Trade allowance fine print
//   - Incentive stacking details
//   - Full raw disclaimer text
//
// Runs weekly (fees don't change daily). Results stored in 'dealer-disclosures' dataset.
// ─────────────────────────────────────────────────────────────────────────────

const BASE = 'inventory-data-bus1/getInventory';
const DI_BASE = 'apis/widget/INVENTORY_LISTING_DEFAULT_AUTO';

const DEALERS = [
    {
        name: 'Walker Chevrolet',
        baseUrl: 'https://www.walkerchevrolet.com',
        inventoryApiUrl: `https://www.walkerchevrolet.com/${DI_BASE}_NEW:${BASE}`,
        platform: 'dealer_com',
    },
    {
        name: 'Chevrolet Buick GMC of Murfreesboro',
        baseUrl: 'https://www.chevroletbuickgmcofmurfreesboro.com',
        inventoryApiUrl: `https://www.chevroletbuickgmcofmurfreesboro.com/${DI_BASE}_NEW:${BASE}`,
        platform: 'dealer_com',
    },
    {
        name: 'Serra Chevrolet Buick GMC Nashville',
        baseUrl: 'https://www.serranashville.com',
        inventoryApiUrl: `https://www.serranashville.com/${DI_BASE}_NEW:${BASE}`,
        platform: 'dealer_com',
    },
    {
        name: 'Darrell Waltrip Buick GMC',
        baseUrl: 'https://www.darrellwaltripbuickgmc.com',
        inventoryApiUrl: `https://www.darrellwaltripbuickgmc.com/${DI_BASE}_NEW:${BASE}`,
        platform: 'dealer_com',
    },
    {
        name: 'Carl Black Chevrolet Nashville',
        baseUrl: 'https://www.carlblackchevy.com',
        inventoryApiUrl: null,
        platform: 'dealer_inspire',
    },
];

const HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
};

// How many vehicle detail pages to sample per dealer (more = more accurate, slower)
const PAGES_TO_SAMPLE = 3;

async function fetchWithRetry(url, options = {}, retries = 3) {
    for (let attempt = 1; attempt <= retries; attempt++) {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 30000);
        try {
            const response = await fetch(url, { signal: controller.signal, ...options });
            clearTimeout(timeout);
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            return response;
        } catch (err) {
            clearTimeout(timeout);
            if (attempt === retries) throw err;
            await new Promise(r => setTimeout(r, 2000 * attempt));
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 1: Get sample vehicle detail page URLs from each dealer
// ─────────────────────────────────────────────────────────────────────────────
async function getSampleDetailUrls(dealer) {
    const urls = [];

    if (dealer.platform === 'dealer_com' && dealer.inventoryApiUrl) {
        try {
            const apiUrl = `${dealer.inventoryApiUrl}?pageSize=${PAGES_TO_SAMPLE}&pageStart=0`;
            const response = await fetchWithRetry(apiUrl, {
                headers: { ...HEADERS, 'Accept': 'application/json' },
            });
            const data = await response.json();
            for (const v of (data.inventory || []).slice(0, PAGES_TO_SAMPLE)) {
                const link = v.link || '';
                const fullUrl = link.startsWith('http') ? link : `${dealer.baseUrl}${link}`;
                urls.push({ url: fullUrl, vin: v.vin, model: `${v.year} ${v.make} ${v.model}` });
            }
        } catch (err) {
            console.error(`[${dealer.name}] Failed to get sample URLs: ${err.message}`);
        }
    } else if (dealer.platform === 'dealer_inspire') {
        // For Dealer Inspire, scrape the listing page to get detail URLs
        try {
            const response = await fetchWithRetry(`${dealer.baseUrl}/new-vehicles/`, { headers: HEADERS });
            const html = await response.text();
            const $ = cheerio.load(html);
            $('a[href*="/new/"]').slice(0, PAGES_TO_SAMPLE).each((i, el) => {
                const href = $(el).attr('href') || '';
                const fullUrl = href.startsWith('http') ? href : `${dealer.baseUrl}${href}`;
                if (fullUrl.includes('/new/') && !urls.find(u => u.url === fullUrl)) {
                    urls.push({ url: fullUrl, vin: null, model: null });
                }
            });
        } catch (err) {
            console.error(`[${dealer.name}] Failed to get Dealer Inspire URLs: ${err.message}`);
        }
    }

    return urls;
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 2: Scrape each vehicle detail page for disclosure text
// ─────────────────────────────────────────────────────────────────────────────
async function scrapeDisclosureText(dealer, pageInfo) {
    console.log(`[${dealer.name}] Scraping disclosure from: ${pageInfo.url}`);

    let html;
    try {
        const response = await fetchWithRetry(pageInfo.url, { headers: HEADERS });
        html = await response.text();
    } catch (err) {
        console.error(`[${dealer.name}] Failed to fetch detail page: ${err.message}`);
        return null;
    }

    const $ = cheerio.load(html);

    // Remove scripts and styles to clean up the text
    $('script, style, nav, header, footer').remove();

    // Target disclosure-specific sections
    const disclosureSelectors = [
        // Dealer.com disclosure sections
        '[id*="disclaimer"]', '[class*="disclaimer"]',
        '[id*="disclosure"]', '[class*="disclosure"]',
        '[id*="legal"]', '[class*="legal"]',
        '[id*="fine-print"]', '[class*="fine-print"]',
        // Pricing disclaimer
        '[id*="pricing"]', '[class*="pricing-disclaimer"]',
        // Dealer notes (often contain add-on info)
        '[id*="dealernotes"]', '[class*="dealer-notes"]',
        '[id*="dealer-notes"]',
        // Footnotes
        'footer p', '.footnote', '[class*="footnote"]',
        // Generic small text that often contains disclosures
        'p small', 'small',
    ];

    const disclosureTexts = new Set();

    for (const selector of disclosureSelectors) {
        $(selector).each((i, el) => {
            const text = $(el).text().trim();
            if (text.length > 50) { // Skip very short snippets
                disclosureTexts.add(text);
            }
        });
    }

    // Also grab any paragraph text that mentions fees, doc, financing, trade
    const feeKeywords = /\b(doc fee|documentation fee|dealer fee|market adjustment|add-on|addendum|finance|financing|apr|trade|trade-in|allowance|msrp|disclaimer|not included|tax|title|license|\$\d{2,4})\b/i;
    $('p, li, div').each((i, el) => {
        const text = $(el).text().trim();
        if (text.length > 30 && text.length < 2000 && feeKeywords.test(text)) {
            disclosureTexts.add(text);
        }
    });

    // Combine all unique disclosure text
    const combinedText = Array.from(disclosureTexts).join('\n\n');

    return {
        url: pageInfo.url,
        vin: pageInfo.vin,
        model: pageInfo.model,
        rawDisclosureText: combinedText.slice(0, 8000), // Cap at 8k chars for GPT
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 3: Use GPT to parse structured fee data from raw disclosure text
// ─────────────────────────────────────────────────────────────────────────────
async function parseDisclosuresWithGPT(dealer, rawTexts, openai) {
    const combinedRaw = rawTexts
        .filter(Boolean)
        .map(t => t.rawDisclosureText)
        .join('\n\n---PAGE BREAK---\n\n');

    if (!combinedRaw.trim()) {
        console.log(`[${dealer.name}] No disclosure text found to parse`);
        return null;
    }

    console.log(`[${dealer.name}] Sending ${combinedRaw.length} chars to GPT for parsing...`);

    const prompt = `You are analyzing car dealership disclosure text scraped from their website. 
Extract the following structured information. If a field is not mentioned, use null.
Be precise — only extract what is explicitly stated in the text.

Dealer: ${dealer.name}

DISCLOSURE TEXT:
${combinedRaw}

Extract and return a JSON object with these fields:
{
  "docFee": "dollar amount of documentation/doc fee if mentioned, e.g. '$789'",
  "marketAdjustment": "any market adjustment or ADM (additional dealer markup) mentioned",
  "dealerInstalledAddOns": ["list of dealer-installed add-ons with prices if mentioned, e.g. 'Paint protection $499', 'LoJack $299'"],
  "financingRates": ["list of financing rates mentioned, e.g. '1.9% APR for 36 months'"],
  "tradeAllowanceLanguage": "any fine print about trade-in valuations or allowances",
  "incentivesStacked": ["list of GM or manufacturer incentives mentioned that are being applied"],
  "priceExcludes": ["list of items explicitly excluded from the advertised price"],
  "otherFees": ["any other fees mentioned with amounts"],
  "keyDisclaimer": "the single most important disclaimer sentence in plain English",
  "rawSummary": "2-3 sentence plain English summary of what this dealer's pricing practices and hidden fees look like"
}

Return ONLY valid JSON, no markdown, no explanation.`;

    try {
        const completion = await openai.chat.completions.create({
            model: 'gpt-4.1-mini',
            messages: [{ role: 'user', content: prompt }],
            temperature: 0.1,
            max_tokens: 1000,
        });

        const content = completion.choices[0].message.content.trim();
        // Strip markdown code blocks if present
        const jsonStr = content.replace(/^```json?\n?/, '').replace(/\n?```$/, '');
        return JSON.parse(jsonStr);
    } catch (err) {
        console.error(`[${dealer.name}] GPT parsing failed: ${err.message}`);
        return { rawSummary: 'GPT parsing failed — see rawDisclosureText', error: err.message };
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN ENTRY POINT
// ─────────────────────────────────────────────────────────────────────────────
await Actor.init();

const input = await Actor.getInput() || {};
const openaiApiKey = input.openaiApiKey || process.env.OPENAI_API_KEY;

if (!openaiApiKey) {
    console.error('ERROR: No OpenAI API key provided. Set OPENAI_API_KEY in environment variables or pass via input.');
    await Actor.exit();
    process.exit(1);
}

const openai = new OpenAI({ apiKey: openaiApiKey });

// Which dealers to scrape (default: all)
const targetDealers = (input.dealers && input.dealers.length > 0)
    ? DEALERS.filter(d => input.dealers.some(name => d.name.toLowerCase().includes(name.toLowerCase())))
    : DEALERS;

const dataset = await Actor.openDataset('dealer-disclosures');
const scrapedAt = new Date().toISOString();
const scrapedDate = scrapedAt.split('T')[0];

console.log(`\nDealer Disclosure Scraper`);
console.log(`Date: ${scrapedDate}`);
console.log(`Scraping ${targetDealers.length} dealer(s), ${PAGES_TO_SAMPLE} pages each\n`);

for (const dealer of targetDealers) {
    console.log(`\n═══ ${dealer.name} ═══`);

    // Step 1: Get sample detail page URLs
    const sampleUrls = await getSampleDetailUrls(dealer);
    console.log(`[${dealer.name}] Found ${sampleUrls.length} sample pages to scrape`);

    if (sampleUrls.length === 0) {
        console.warn(`[${dealer.name}] No pages found, skipping`);
        continue;
    }

    // Step 2: Scrape disclosure text from each page
    const rawTexts = [];
    for (const pageInfo of sampleUrls) {
        const result = await scrapeDisclosureText(dealer, pageInfo);
        if (result) rawTexts.push(result);
        await new Promise(r => setTimeout(r, 1500)); // Polite delay between requests
    }

    // Step 3: Parse with GPT
    const parsed = await parseDisclosuresWithGPT(dealer, rawTexts, openai);

    // Step 4: Save to dataset
    const record = {
        dealer: dealer.name,
        scrapedAt,
        scrapedDate,
        pagesSampled: rawTexts.length,
        sampleUrls: sampleUrls.map(u => u.url),
        ...parsed,
        rawDisclosureTexts: rawTexts.map(t => ({
            url: t.url,
            vin: t.vin,
            model: t.model,
            text: t.rawDisclosureText,
        })),
    };

    await dataset.pushData(record);

    // Print summary to log
    console.log(`[${dealer.name}] Results:`);
    if (parsed) {
        console.log(`  Doc Fee: ${parsed.docFee || 'Not found'}`);
        console.log(`  Market Adjustment: ${parsed.marketAdjustment || 'None mentioned'}`);
        console.log(`  Add-ons: ${(parsed.dealerInstalledAddOns || []).join(', ') || 'None mentioned'}`);
        console.log(`  Financing: ${(parsed.financingRates || []).join(', ') || 'None mentioned'}`);
        console.log(`  Summary: ${parsed.rawSummary || 'N/A'}`);
    }
}

console.log(`\n✓ Disclosure scraper complete. Results saved to 'dealer-disclosures' dataset.`);
await Actor.exit();
