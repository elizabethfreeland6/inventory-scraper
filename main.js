import { Actor } from 'apify';
import * as cheerio from 'cheerio';

// Native fetch wrapper with retry and timeout (no external HTTP library needed)
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
            console.warn(`Attempt ${attempt} failed for ${url}: ${err.message}. Retrying...`);
            await new Promise(r => setTimeout(r, 2000 * attempt));
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DEALER INVENTORY SCRAPER
// Supports:
//   • Dealer.com platform  (Freeland, Walker, Murfreesboro, Serra, Darrell Waltrip)
//   • Dealer Inspire platform (Carl Black)
//
// DAYS IN STOCK TRACKING:
//   Uses Apify Key-Value Store to persist VIN first-seen dates across runs.
//   Each run calculates daysOnLot for every vehicle and records sold vehicles.
// ─────────────────────────────────────────────────────────────────────────────

const BASE = 'inventory-data-bus1/getInventory';
const DI_BASE = 'apis/widget/INVENTORY_LISTING_DEFAULT_AUTO';

const DEALERS = [
    {
        name: 'Freeland Chevrolet',
        platform: 'dealer_com',
        baseUrl: 'https://www.freelandchevrolet.com',
        isOwnDealership: true,  // Flag to distinguish our inventory from competitors
        apiUrlNew:  `https://www.freelandchevrolet.com/${DI_BASE}_NEW:${BASE}`,
        apiUrlUsed: `https://www.freelandchevrolet.com/${DI_BASE}_USED:${BASE}`,
        apiUrlAll:  `https://www.freelandchevrolet.com/${DI_BASE}_ALL:${BASE}`,
    },
    {
        name: 'Carl Black Chevrolet Nashville',
        platform: 'dealer_inspire',
        baseUrl: 'https://www.carlblackchevy.com',
        isOwnDealership: false,
        // Dealer Inspire: condition handled via URL path
        apiUrlNew:  null, // uses HTML scrape at /new-vehicles/
        apiUrlUsed: null, // uses HTML scrape at /used-vehicles/
        apiUrlAll:  null, // uses HTML scrape at /all-inventory/
    },
    {
        name: 'Walker Chevrolet',
        platform: 'dealer_com',
        baseUrl: 'https://www.walkerchevrolet.com',
        isOwnDealership: false,
        apiUrlNew:  `https://www.walkerchevrolet.com/${DI_BASE}_NEW:${BASE}`,
        apiUrlUsed: `https://www.walkerchevrolet.com/${DI_BASE}_USED:${BASE}`,
        apiUrlAll:  `https://www.walkerchevrolet.com/${DI_BASE}_ALL:${BASE}`,
    },
    {
        name: 'Chevrolet Buick GMC of Murfreesboro',
        platform: 'dealer_com',
        baseUrl: 'https://www.chevroletbuickgmcofmurfreesboro.com',
        isOwnDealership: false,
        apiUrlNew:  `https://www.chevroletbuickgmcofmurfreesboro.com/${DI_BASE}_NEW:${BASE}`,
        apiUrlUsed: `https://www.chevroletbuickgmcofmurfreesboro.com/${DI_BASE}_USED:${BASE}`,
        apiUrlAll:  `https://www.chevroletbuickgmcofmurfreesboro.com/${DI_BASE}_ALL:${BASE}`,
    },
    {
        name: 'Serra Chevrolet Buick GMC Nashville',
        platform: 'dealer_com',
        baseUrl: 'https://www.serranashville.com',
        isOwnDealership: false,
        apiUrlNew:  `https://www.serranashville.com/${DI_BASE}_NEW:${BASE}`,
        apiUrlUsed: `https://www.serranashville.com/${DI_BASE}_USED:${BASE}`,
        apiUrlAll:  `https://www.serranashville.com/${DI_BASE}_ALL:${BASE}`,
    },
    {
        name: 'Darrell Waltrip Buick GMC',
        platform: 'dealer_com',
        baseUrl: 'https://www.darrellwaltripbuickgmc.com',
        isOwnDealership: false,
        // NOTE: Both the ALL and USED endpoints aggregate the entire Waltrip auto group
        // (Honda, Toyota, Audi, etc.) returning 6,000+ vehicles. The Dealer.com API has no
        // rooftop-level URL filter. We use the NEW endpoint directly and apply a post-fetch
        // make filter on the USED endpoint to keep only Buick/GMC/Chevrolet.
        apiUrlNew:  `https://www.darrellwaltripbuickgmc.com/${DI_BASE}_NEW:${BASE}`,
        apiUrlUsed: `https://www.darrellwaltripbuickgmc.com/${DI_BASE}_USED:${BASE}`,
        apiUrlAll:  null, // intentionally disabled — would pull 6,000+ group-wide vehicles
        // IMPORTANT: The _USED endpoint returns the entire Waltrip auto group (~6,000 vehicles
        // including Honda, Toyota, Audi, etc.). The Dealer.com API has no rooftop-level filter.
        // We apply a post-fetch make filter in the main loop to keep only Buick/GMC/Chevrolet.
        usedMakeFilter: ['Buick', 'GMC', 'Chevrolet'],
    },
];

const HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/html, */*',
    'Accept-Language': 'en-US,en;q=0.9',
};

// ─────────────────────────────────────────────────────────────────────────────
// DAYS IN STOCK TRACKING
// Uses Apify Key-Value Store (persists between runs) to track:
//   - firstSeenDate: when a VIN was first scraped
//   - daysOnLot: calculated each run as (today - firstSeenDate)
//   - soldVehicles: VINs that disappeared since last run
//
// KV Store key format: "vin_history" → { [vin]: { firstSeenDate, dealer, make, model, year, trim } }
// ─────────────────────────────────────────────────────────────────────────────

const KV_HISTORY_KEY = 'vin_history';

async function loadVinHistory(kvStore) {
    const history = await kvStore.getValue(KV_HISTORY_KEY);
    return history || {};
}

async function saveVinHistory(kvStore, history) {
    await kvStore.setValue(KV_HISTORY_KEY, history);
}

function applyDaysInStock(vehicles, vinHistory, today) {
    const todayStr = today.toISOString().split('T')[0]; // YYYY-MM-DD

    for (const vehicle of vehicles) {
        if (!vehicle.vin) continue;

        // Get the current price as a number for comparison
        const currentPrice = parseFloat(
            String(vehicle.dealerPrice || vehicle.msrp || '').replace(/[$,]/g, '')
        ) || null;

        if (vinHistory[vehicle.vin]) {
            const record = vinHistory[vehicle.vin];

            // ── Days on lot ──
            const firstSeen = new Date(record.firstSeenDate);
            const msOnLot = today - firstSeen;
            vehicle.firstSeenDate = record.firstSeenDate;
            vehicle.daysOnLot = Math.floor(msOnLot / (1000 * 60 * 60 * 24));

            // ── Price history tracking ──
            const priceHistory = record.priceHistory || [];
            const lastEntry = priceHistory[priceHistory.length - 1];
            const lastPrice = lastEntry ? lastEntry.price : null;

            // Only add a new entry if price changed since last run
            if (currentPrice !== null && currentPrice !== lastPrice) {
                priceHistory.push({ date: todayStr, price: currentPrice });
                record.priceHistory = priceHistory;
            }

            // Attach price history summary to vehicle record
            vehicle.priceHistory = priceHistory;
            vehicle.originalPrice = priceHistory.length > 0 ? priceHistory[0].price : currentPrice;
            vehicle.currentPrice = currentPrice;
            vehicle.priceDrop = (vehicle.originalPrice && currentPrice)
                ? vehicle.originalPrice - currentPrice
                : 0;
            vehicle.priceDropCount = priceHistory.length > 1 ? priceHistory.length - 1 : 0;
            vehicle.lastPriceChangeDate = priceHistory.length > 1
                ? priceHistory[priceHistory.length - 1].date
                : null;

        } else {
            // New VIN — record today as first seen
            vehicle.firstSeenDate = todayStr;
            vehicle.daysOnLot = 0;
            vehicle.priceHistory = currentPrice ? [{ date: todayStr, price: currentPrice }] : [];
            vehicle.originalPrice = currentPrice;
            vehicle.currentPrice = currentPrice;
            vehicle.priceDrop = 0;
            vehicle.priceDropCount = 0;
            vehicle.lastPriceChangeDate = null;

            vinHistory[vehicle.vin] = {
                firstSeenDate: todayStr,
                dealer: vehicle.dealer,
                make: vehicle.make,
                model: vehicle.model,
                year: vehicle.year,
                trim: vehicle.trim,
                condition: vehicle.condition,
                priceHistory: currentPrice ? [{ date: todayStr, price: currentPrice }] : [],
            };
        }

        // Age bucket for easy filtering
        vehicle.ageBucket = getAgeBucket(vehicle.daysOnLot);
    }

    return vehicles;
}

function getAgeBucket(days) {
    if (days === null || days === undefined) return 'Unknown';
    if (days <= 15)  return '0-15 days';
    if (days <= 30)  return '16-30 days';
    if (days <= 45)  return '31-45 days';
    if (days <= 60)  return '46-60 days';
    if (days <= 90)  return '61-90 days';
    if (days <= 120) return '91-120 days';
    return '120+ days';
}

function detectSoldVehicles(vinHistory, currentVins, today) {
    const todayStr = today.toISOString().split('T')[0];
    const soldVehicles = [];

    for (const [vin, record] of Object.entries(vinHistory)) {
        if (!currentVins.has(vin) && !record.soldDate) {
            // VIN was in history but not in today's scrape — mark as sold
            const firstSeen = new Date(record.firstSeenDate);
            const daysOnLot = Math.floor((today - firstSeen) / (1000 * 60 * 60 * 24));
            const priceHistory = record.priceHistory || [];
            const originalPrice = priceHistory.length > 0 ? priceHistory[0].price : null;
            const finalPrice = priceHistory.length > 0 ? priceHistory[priceHistory.length - 1].price : null;
            soldVehicles.push({
                vin,
                dealer: record.dealer,
                make: record.make,
                model: record.model,
                year: record.year,
                trim: record.trim,
                condition: record.condition,
                firstSeenDate: record.firstSeenDate,
                soldDate: todayStr,
                daysOnLot,
                ageBucket: getAgeBucket(daysOnLot),
                status: 'Sold/Removed',
                originalPrice,
                finalPrice,
                totalPriceDrop: (originalPrice && finalPrice) ? originalPrice - finalPrice : 0,
                priceDropCount: priceHistory.length > 1 ? priceHistory.length - 1 : 0,
                priceHistory,
            });
            // Mark as sold in history so we don't re-report it
            vinHistory[vin].soldDate = todayStr;
            vinHistory[vin].daysOnLot = daysOnLot;
        }
    }

    return soldVehicles;
}

// ─────────────────────────────────────────────────────────────────────────────
// DEALER.COM SCRAPER
// Uses the undocumented but publicly accessible JSON API endpoint.
// Paginates through all results using pageStart parameter.
// ─────────────────────────────────────────────────────────────────────────────
async function scrapeDealerCom(dealer, input) {
    const rawVehicles = [];
    const condition = (input.condition || 'new').toLowerCase();

    // Select the correct endpoint(s) based on condition filter.
    // Each dealer now has apiUrlNew, apiUrlUsed, and apiUrlAll.
    // Darrell Waltrip has apiUrlAll = null (their ALL endpoint aggregates a dealer group).
    let endpoints = [];
    if (condition === 'new') {
        endpoints = dealer.apiUrlNew ? [{ url: dealer.apiUrlNew, endpointCondition: 'new' }] : [];
    } else if (condition === 'used') {
        endpoints = dealer.apiUrlUsed ? [{ url: dealer.apiUrlUsed, endpointCondition: 'used' }] : [];
    } else {
        // 'all' — prefer the combined endpoint; fall back to new+used if not available
        if (dealer.apiUrlAll) {
            endpoints = [{ url: dealer.apiUrlAll, endpointCondition: 'all' }];
        } else {
            if (dealer.apiUrlNew) endpoints.push({ url: dealer.apiUrlNew, endpointCondition: 'new' });
            if (dealer.apiUrlUsed) endpoints.push({ url: dealer.apiUrlUsed, endpointCondition: 'used' });
        }
    }

    for (const ep of endpoints) {
        await scrapeDealerComEndpoint(dealer, ep.url, input, rawVehicles);
    }

    // Deduplicate by VIN within this dealer (guards against endpoint overlap)
    const seen = new Set();
    const vehicles = [];
    for (const v of rawVehicles) {
        const key = v.vin || `${v.stockNumber}_${v.year}_${v.model}`;
        if (!seen.has(key)) {
            seen.add(key);
            vehicles.push(v);
        }
    }
    if (rawVehicles.length !== vehicles.length) {
        console.log(`[${dealer.name}] Deduplication removed ${rawVehicles.length - vehicles.length} duplicate records`);
    }

    console.log(`[${dealer.name}] Total after all endpoints: ${vehicles.length} vehicles`);
    return vehicles;
}

async function scrapeDealerComEndpoint(dealer, apiUrl, input, vehicles) {
    let pageStart = 0;
    let totalCount = null;
    const pageSize = 100; // Request max per page to minimize requests
    // Track VINs seen so far in THIS endpoint call to detect looping pages.
    // Some Dealer.com _ALL endpoints report a large totalCount but loop back to
    // the same vehicles once pageStart exceeds the actual inventory size.
    const seenVinsThisEndpoint = new Set();

    console.log(`[${dealer.name}] Starting Dealer.com scrape from ${apiUrl}...`);

    do {
        const url = `${apiUrl}?pageSize=${pageSize}&pageStart=${pageStart}`;
        console.log(`[${dealer.name}] Fetching page starting at ${pageStart}...`);

        let data;
        try {
            const response = await fetchWithRetry(url, { headers: HEADERS });
            data = await response.json();
        } catch (err) {
            console.error(`[${dealer.name}] Request failed: ${err.message}`);
            break;
        }
        if (!data || !data.pageInfo) {
            console.error(`[${dealer.name}] Unexpected response structure`);
            break;
        }

        if (totalCount === null) {
            totalCount = data.pageInfo.totalCount;
            console.log(`[${dealer.name}] Total vehicles reported by API: ${totalCount}`);
        }

        const pageVehicles = data.inventory || [];
        if (pageVehicles.length === 0) break;

        // Check if this page is a repeat — Dealer.com loops back to page 0 once
        // pageStart exceeds actual inventory size, causing duplicate records.
        // We stop ONLY when ALL VINs on the page have been seen before (full repeat).
        // A partial overlap is normal when inventory changes between pages.
        const pageVins = pageVehicles.map(v => v.vin).filter(Boolean);
        const newVinsOnPage = pageVins.filter(vin => !seenVinsThisEndpoint.has(vin));
        if (pageVins.length > 0 && newVinsOnPage.length === 0) {
            // Every VIN on this page was already collected — we've looped back to the start.
            console.log(`[${dealer.name}] Page at ${pageStart} is a full repeat (all ${pageVins.length} VINs already seen) — stopping. API totalCount=${totalCount}, actual inventory ~${seenVinsThisEndpoint.size}.`);
            break;
        }

        // Add only the new VINs to our seen set
        for (const vin of newVinsOnPage) seenVinsThisEndpoint.add(vin);

        for (const v of pageVehicles) {
            const vehicle = parseDealerComVehicle(v, dealer);
            // Apply general filters from input
            if (shouldInclude(vehicle, input)) {
                vehicles.push(vehicle);
            }
        }

        pageStart += pageSize;
    } while (pageStart < totalCount);

    console.log(`[${dealer.name}] Endpoint scraped, running total: ${vehicles.length} vehicles`);
}

function parseDealerComVehicle(v, dealer) {
    // Extract attributes array into a flat object
    const attrs = {};
    for (const attr of (v.attributes || [])) {
        attrs[attr.name] = attr.value;
    }

    // Extract pricing
    const pricing = v.pricing || {};
    const msrp = pricing.retailPrice || null;
    let dealerPrice = null;
    let finalPrice = null;
    for (const dp of (pricing.dprice || [])) {
        if (dp.isFinalPrice) finalPrice = dp.value;
        if (dp.typeClass === 'wholesalePrice') dealerPrice = dp.value;
    }

    // Build detail page URL
    const detailPath = v.link || '';
    const detailUrl = detailPath.startsWith('http') ? detailPath : `${dealer.baseUrl}${detailPath}`;

    // Primary photo
    const images = v.images || [];
    const primaryPhoto = images.length > 0 ? images[0].uri : null;

    return {
        dealer: dealer.name,
        isOwnDealership: dealer.isOwnDealership || false,
        platform: 'Dealer.com',
        // accountId identifies the specific rooftop within a dealer group.
        // For group-wide endpoints (e.g. Darrell Waltrip), this is the same for all vehicles
        // because the API does not expose per-rooftop identifiers — filtering must be done by make.
        accountId: v.accountId || null,
        condition: v.condition || null,
        year: v.year || null,
        make: v.make || null,
        model: v.model || null,
        trim: v.trim || null,
        bodyStyle: v.bodyStyle || null,
        fuelType: v.fuelType || null,
        vin: v.vin || null,
        stockNumber: v.stockNumber || null,
        status: parseStatus(v.status),
        exteriorColor: attrs.exteriorColor || null,
        interiorColor: attrs.interiorColor || null,
        engine: attrs.engine || null,
        transmission: attrs.transmission || null,
        drivetrain: attrs.normalDriveLine || null,
        mileage: attrs.odometer || null,
        mpgCity: attrs.fuelEconomy ? attrs.fuelEconomy.split('/')[0]?.trim() : null,
        mpgHighway: attrs.fuelEconomy ? attrs.fuelEconomy.split('/')[1]?.trim() : null,
        msrp: msrp,
        dealerPrice: dealerPrice || finalPrice,
        primaryPhotoUrl: primaryPhoto,
        photoCount: images.length,
        detailUrl: detailUrl,
        scrapedAt: new Date().toISOString(),
        // Days in stock fields — populated later by applyDaysInStock()
        firstSeenDate: null,
        daysOnLot: null,
        ageBucket: null,
    };
}

function parseStatus(statusCode) {
    // Dealer.com status codes: 1 = On Lot, 7 = In Transit, 2 = On Order
    const statusMap = { 1: 'On Lot', 7: 'In Transit', 2: 'On Order' };
    return statusMap[statusCode] || `Unknown (${statusCode})`;
}

// ─────────────────────────────────────────────────────────────────────────────
// DEALER INSPIRE SCRAPER
// Carl Black uses Dealer Inspire (WordPress-based platform).
// Scrapes the inventory listing pages directly via HTML.
// ─────────────────────────────────────────────────────────────────────────────
async function scrapeDealerInspire(dealer, input) {
    const vehicles = [];
    let page = 1;
    let hasMore = true;

    // Build base inventory URL with condition filter
    const condition = input.condition || 'all';
    let inventoryPath = '/new-vehicles/';
    if (condition === 'used') inventoryPath = '/used-vehicles/';
    else if (condition === 'all') inventoryPath = '/all-inventory/';

    console.log(`[${dealer.name}] Starting Dealer Inspire scrape...`);

    while (hasMore) {
        const url = `${dealer.baseUrl}${inventoryPath}?_p=${page}`;
        console.log(`[${dealer.name}] Fetching page ${page}...`);

        let html;
        try {
            const response = await fetchWithRetry(url, {
                headers: { ...HEADERS, 'Accept': 'text/html,application/xhtml+xml' },
            });
            html = await response.text();
        } catch (err) {
            console.error(`[${dealer.name}] Request failed: ${err.message}`);
            break;
        }

        const $ = cheerio.load(html);

        // Dealer Inspire vehicle cards
        const cards = $('.di-vehicle-card, [class*="vehicle-card"], .inventory-listing-item, .vehicle');
        console.log(`[${dealer.name}] Page ${page}: found ${cards.length} vehicle cards`);

        if (cards.length === 0) {
            // Try JSON-LD structured data as fallback
            const jsonLdVehicles = extractJsonLdVehicles($, dealer);
            if (jsonLdVehicles.length > 0) {
                for (const v of jsonLdVehicles) {
                    if (shouldInclude(v, input)) vehicles.push(v);
                }
            }
            hasMore = false;
            break;
        }

        cards.each((i, el) => {
            const vehicle = parseDealerInspireCard($, el, dealer);
            if (vehicle && shouldInclude(vehicle, input)) {
                vehicles.push(vehicle);
            }
        });

        // Check for next page
        const nextBtn = $('.pagination .next, a[rel="next"], .di-pagination .next');
        hasMore = nextBtn.length > 0 && page < 50; // Safety cap at 50 pages
        page++;
    }

    console.log(`[${dealer.name}] Scraped ${vehicles.length} vehicles (after filters)`);
    return vehicles;
}

function parseDealerInspireCard($, el, dealer) {
    const $el = $(el);

    // Extract data attributes (Dealer Inspire stores data in data-* attributes)
    const dataVin = $el.attr('data-vin') || $el.find('[data-vin]').attr('data-vin');
    const dataYear = $el.attr('data-year') || $el.find('[data-year]').attr('data-year');
    const dataMake = $el.attr('data-make') || $el.find('[data-make]').attr('data-make');
    const dataModel = $el.attr('data-model') || $el.find('[data-model]').attr('data-model');
    const dataTrim = $el.attr('data-trim') || $el.find('[data-trim]').attr('data-trim');
    const dataCondition = $el.attr('data-condition') || $el.find('[data-condition]').attr('data-condition');
    const dataStock = $el.attr('data-stock') || $el.find('[data-stock]').attr('data-stock');
    const dataPrice = $el.attr('data-price') || $el.find('[data-price]').attr('data-price');
    const dataMileage = $el.attr('data-mileage') || $el.find('[data-mileage]').attr('data-mileage');

    // Text extraction fallbacks
    const titleText = $el.find('.vehicle-title, h2, h3, .title').first().text().trim();
    const priceText = $el.find('.price, .vehicle-price, [class*="price"]').first().text().trim();
    const linkEl = $el.find('a').first();
    const href = linkEl.attr('href') || '';
    const detailUrl = href.startsWith('http') ? href : `${dealer.baseUrl}${href}`;
    const imgSrc = $el.find('img').first().attr('src') || $el.find('img').first().attr('data-src');

    // Parse price from text if not in data attribute
    const priceMatch = priceText.match(/\$[\d,]+/);
    const parsedPrice = dataPrice || (priceMatch ? priceMatch[0] : null);

    // Parse year/make/model from title if data attributes not present
    let year = dataYear, make = dataMake, model = dataModel;
    if (!year && titleText) {
        const titleMatch = titleText.match(/^(\d{4})\s+(\w+)\s+(.+)/);
        if (titleMatch) {
            year = titleMatch[1];
            make = make || titleMatch[2];
            model = model || titleMatch[3].split(' ')[0];
        }
    }

    if (!dataVin && !titleText) return null; // Skip empty cards

    return {
        dealer: dealer.name,
        isOwnDealership: dealer.isOwnDealership || false,
        platform: 'Dealer Inspire',
        condition: dataCondition || 'New',
        year: year ? parseInt(year) : null,
        make: make || null,
        model: model || null,
        trim: dataTrim || null,
        bodyStyle: null,
        fuelType: null,
        vin: dataVin || null,
        stockNumber: dataStock || null,
        status: 'On Lot',
        exteriorColor: $el.attr('data-color') || $el.find('[data-color]').attr('data-color') || null,
        interiorColor: null,
        engine: null,
        transmission: null,
        drivetrain: null,
        mileage: dataMileage ? `${dataMileage} miles` : null,
        mpgCity: null,
        mpgHighway: null,
        msrp: parsedPrice,
        dealerPrice: parsedPrice,
        primaryPhotoUrl: imgSrc || null,
        photoCount: null,
        detailUrl: detailUrl,
        scrapedAt: new Date().toISOString(),
        // Days in stock fields — populated later by applyDaysInStock()
        firstSeenDate: null,
        daysOnLot: null,
        ageBucket: null,
    };
}

function extractJsonLdVehicles($, dealer) {
    const vehicles = [];
    $('script[type="application/ld+json"]').each((i, el) => {
        try {
            const data = JSON.parse($(el).html());
            const items = Array.isArray(data) ? data : [data];
            for (const item of items) {
                if (item['@type'] === 'Car' || item['@type'] === 'Vehicle') {
                    vehicles.push({
                        dealer: dealer.name,
                        isOwnDealership: dealer.isOwnDealership || false,
                        platform: 'Dealer Inspire',
                        condition: item.itemCondition?.includes('New') ? 'New' : 'Used',
                        year: item.modelDate ? parseInt(item.modelDate) : null,
                        make: item.brand?.name || null,
                        model: item.model || null,
                        trim: null,
                        bodyStyle: item.bodyType || null,
                        fuelType: item.fuelType || null,
                        vin: item.vehicleIdentificationNumber || null,
                        stockNumber: null,
                        status: 'On Lot',
                        exteriorColor: item.color || null,
                        interiorColor: null,
                        engine: null,
                        transmission: item.vehicleTransmission || null,
                        drivetrain: item.driveWheelConfiguration || null,
                        mileage: item.mileageFromOdometer?.value ? `${item.mileageFromOdometer.value} miles` : null,
                        mpgCity: null,
                        mpgHighway: null,
                        msrp: item.offers?.price ? `$${item.offers.price}` : null,
                        dealerPrice: item.offers?.price ? `$${item.offers.price}` : null,
                        primaryPhotoUrl: item.image || null,
                        photoCount: null,
                        detailUrl: item.url || null,
                        scrapedAt: new Date().toISOString(),
                        firstSeenDate: null,
                        daysOnLot: null,
                        ageBucket: null,
                    });
                }
            }
        } catch (e) {
            // Skip malformed JSON-LD
        }
    });
    return vehicles;
}

// ─────────────────────────────────────────────────────────────────────────────
// FILTER LOGIC
// ─────────────────────────────────────────────────────────────────────────────
function shouldInclude(vehicle, input) {
    if (input.condition && input.condition !== 'all') {
        const cond = (vehicle.condition || '').toLowerCase();
        if (input.condition === 'new' && cond !== 'new') return false;
        if (input.condition === 'used' && cond === 'new') return false;
    }
    if (input.make) {
        if ((vehicle.make || '').toLowerCase() !== input.make.toLowerCase()) return false;
    }
    if (input.model) {
        if (!(vehicle.model || '').toLowerCase().includes(input.model.toLowerCase())) return false;
    }
    if (input.minYear) {
        if (!vehicle.year || vehicle.year < input.minYear) return false;
    }
    if (input.maxYear) {
        if (!vehicle.year || vehicle.year > input.maxYear) return false;
    }
    if (input.maxPrice) {
        const price = parseFloat((vehicle.dealerPrice || vehicle.msrp || '').replace(/[$,]/g, ''));
        if (!isNaN(price) && price > input.maxPrice) return false;
    }
    if (input.minPrice) {
        const price = parseFloat((vehicle.dealerPrice || vehicle.msrp || '').replace(/[$,]/g, ''));
        if (!isNaN(price) && price < input.minPrice) return false;
    }
    return true;
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN ENTRY POINT
// ─────────────────────────────────────────────────────────────────────────────
await Actor.init();

const input = await Actor.getInput() || {};

// Which dealers to scrape (default: all)
const targetDealers = (input.dealers && input.dealers.length > 0)
    ? DEALERS.filter(d => input.dealers.some(name => d.name.toLowerCase().includes(name.toLowerCase())))
    : DEALERS;

console.log('Target dealers:', targetDealers.map(d => `${d.name} (isOwn=${d.isOwnDealership})`).join(', '));

console.log(`Scraping ${targetDealers.length} dealer(s)...`);
console.log(`Filters: condition=${input.condition || 'all'}, make=${input.make || 'any'}, model=${input.model || 'any'}`);

// Open persistent KV store for days-in-stock tracking
const kvStore = await Actor.openKeyValueStore('vin-history');
const vinHistory = await loadVinHistory(kvStore);
const today = new Date();
const todayStr = today.toISOString().split('T')[0];
console.log(`Loaded VIN history: ${Object.keys(vinHistory).length} VINs tracked so far`);

const inventoryDataset = await Actor.openDataset('inventory');
const soldDataset = await Actor.openDataset('sold-vehicles');

let totalSaved = 0;
const allCurrentVins = new Set();
const dealerResults = {}; // Track per-dealer results for summary

// ── Scrape all dealers ──
for (const dealer of targetDealers) {
    let vehicles = [];
    dealerResults[dealer.name] = { count: 0, error: null, isOwn: dealer.isOwnDealership };

    try {
        if (dealer.platform === 'dealer_com') {
            vehicles = await scrapeDealerCom(dealer, input);
        } else if (dealer.platform === 'dealer_inspire') {
            vehicles = await scrapeDealerInspire(dealer, input);
        }
    } catch (err) {
        console.error(`[${dealer.name}] Fatal error: ${err.message}`);
        dealerResults[dealer.name].error = err.message;
    }

    // ── Post-fetch make filter (for dealer groups with shared used inventory endpoints) ──
    // The Dealer.com API does not support rooftop-level filtering via URL parameters.
    // For dealers like Darrell Waltrip whose _USED endpoint returns the entire auto group
    // (Honda, Toyota, Audi, etc.), we filter to only the brands sold at this specific store.
    if (dealer.usedMakeFilter && vehicles.length > 0) {
        const before = vehicles.length;
        const allowed = dealer.usedMakeFilter.map(m => m.toLowerCase());
        vehicles = vehicles.filter(v => {
            const cond = (v.condition || '').toLowerCase();
            if (cond === 'new') return true; // Never filter new vehicles by make
            return allowed.includes((v.make || '').toLowerCase());
        });
        const removed = before - vehicles.length;
        if (removed > 0) {
            console.log(`[${dealer.name}] Make filter: removed ${removed} non-${dealer.usedMakeFilter.join('/')} used vehicles (${before} → ${vehicles.length})`);
        }
    }

    // Track all VINs seen this run
    for (const v of vehicles) {
        if (v.vin) allCurrentVins.add(v.vin);
    }

    // Apply days-in-stock calculations
    vehicles = applyDaysInStock(vehicles, vinHistory, today);

    if (vehicles.length > 0) {
        await inventoryDataset.pushData(vehicles);
        totalSaved += vehicles.length;
        dealerResults[dealer.name].count = vehicles.length;
        const avgDays = vehicles
            .filter(v => v.daysOnLot !== null)
            .reduce((sum, v, _, arr) => sum + v.daysOnLot / arr.length, 0);
        const ownCount = vehicles.filter(v => v.isOwnDealership).length;
        console.log(`[${dealer.name}] Saved ${vehicles.length} vehicles | isOwn=${dealer.isOwnDealership} | ownTagged=${ownCount} | Avg days on lot: ${Math.round(avgDays)}`);
    } else {
        console.warn(`[${dealer.name}] WARNING: 0 vehicles scraped! isOwn=${dealer.isOwnDealership}`);
    }
}

// ── Detect sold/removed vehicles ──
if (Object.keys(vinHistory).length > 0) {
    const soldVehicles = detectSoldVehicles(vinHistory, allCurrentVins, today);
    if (soldVehicles.length > 0) {
        await soldDataset.pushData(soldVehicles);
        console.log(`\nDetected ${soldVehicles.length} sold/removed vehicles since last run`);
        for (const sv of soldVehicles.slice(0, 10)) {
            console.log(`  SOLD: [${sv.dealer}] ${sv.year} ${sv.make} ${sv.model} ${sv.trim || ''} — ${sv.daysOnLot} days on lot`);
        }
    } else {
        console.log('\nNo vehicles sold/removed since last run (or this is the first run)');
    }
}

// ── Save updated VIN history ──
await saveVinHistory(kvStore, vinHistory);
console.log(`VIN history updated: ${Object.keys(vinHistory).length} total VINs tracked`);

console.log(`\n═══════════════════════════════════════`);
console.log(`Run complete: ${todayStr}`);
console.log(`Total vehicles saved: ${totalSaved}`);
console.log(`Total VINs in history: ${Object.keys(vinHistory).length}`);
console.log('\nPer-dealer summary:');
for (const [name, result] of Object.entries(dealerResults)) {
    const status = result.error ? `ERROR: ${result.error}` : `${result.count} vehicles`;
    const ownFlag = result.isOwn ? ' [OWN DEALERSHIP]' : '';
    console.log(`  ${name}${ownFlag}: ${status}`);
}
console.log(`═══════════════════════════════════════`);

await Actor.exit();
