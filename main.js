import { Actor } from 'apify';
import { gotScraping } from 'got-scraping';
import * as cheerio from 'cheerio';

// Global proxy configuration - initialized after Actor.init()
let proxyConfig = null;

// ─────────────────────────────────────────────────────────────────────────────
// HTTP HELPERS
// ─────────────────────────────────────────────────────────────────────────────

// Plain fetch with retry - used for non-Dealer.com endpoints (Algolia, etc.)
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

// gotScrapingFetch: uses got-scraping for browser TLS fingerprinting.
// Used for Dealer.com requests to bypass Akamai CDN bot detection.
async function gotScrapingFetch(url, headers, retries = 4) {
    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            const proxyUrl = proxyConfig ? await proxyConfig.newUrl() : undefined;
            const response = await gotScraping({
                url,
                headers,
                responseType: 'text',
                timeout: { request: 30000 },
                ...(proxyUrl ? { proxyUrl } : {}),
            });
            const statusCode = response.statusCode;
            if (statusCode === 429 || statusCode === 503) {
                const delay = 5000 * attempt;
                console.warn(`[gotScraping] HTTP ${statusCode} at ${url}. Rate limited. Waiting ${delay}ms...`);
                await new Promise(r => setTimeout(r, delay));
                continue;
            }
            if (statusCode >= 400) {
                console.warn(`[gotScraping] HTTP ${statusCode} at ${url}. Body: ${String(response.body).slice(0, 200)}`);
                if (attempt === retries) return null;
                await new Promise(r => setTimeout(r, 3000 * attempt));
                continue;
            }
            try {
                return JSON.parse(response.body);
            } catch (parseErr) {
                console.warn(`[gotScraping] JSON parse error at ${url}: ${parseErr.message}. Body: ${String(response.body).slice(0, 200)}`);
                if (attempt === retries) return null;
                await new Promise(r => setTimeout(r, 2000 * attempt));
                continue;
            }
        } catch (err) {
            if (attempt === retries) {
                console.error(`[gotScraping] All ${retries} attempts failed for ${url}: ${err.message}`);
                return null;
            }
            console.warn(`[gotScraping] Attempt ${attempt} failed for ${url}: ${err.message}. Retrying in ${2 * attempt}s...`);
            await new Promise(r => setTimeout(r, 2000 * attempt));
        }
    }
    return null;
}

// WIS POST fetch: uses the Dealer.com Web Inventory Service (WIS) POST endpoint.
// This is the actual API the website uses internally for pagination.
// Uses "start" parameter (not "pageStart") as an array of strings.
async function wisPostFetch(baseUrl, siteId, condition, startOffset, pageSize, retries = 4) {
    const url = `${baseUrl}/api/widget/ws-inv-data/getInventory`;
    const listingConfigId = condition === 'used' ? 'auto-used' : 'auto-new';
    const pageAlias = condition === 'used'
        ? 'INVENTORY_LISTING_DEFAULT_AUTO_USED'
        : 'INVENTORY_LISTING_DEFAULT_AUTO_NEW';

    const body = {
        siteId,
        locale: 'en_US',
        device: 'DESKTOP',
        pageAlias,
        pageId: `${siteId}_SITEBUILDER_INVENTORY_SEARCH_RESULTS_AUTO_${condition === 'used' ? 'USED' : 'NEW'}_V1_1`,
        windowId: 'inventory-data-bus2',
        widgetName: 'ws-inv-data',
        inventoryParameters: {
            sortBy: ['internetPrice asc'],
            ...(startOffset > 0 ? { start: [String(startOffset)] } : {}),
        },
        preferences: {
            pageSize: String(pageSize),
            'listing.config.id': listingConfigId,
            'showFranchiseVehiclesOnly': 'true',
            'suppressAllConditions': 'compliant',
            'removeEmptyFacets': 'true',
            'removeEmptyConstraints': 'true',
            'required.display.attributes': 'accountId,accountName,askingPrice,bed,bodyStyle,cab,categoryName,certified,cityMpg,classification,classificationName,comments,daysOnLot,doors,driveLine,engine,engineSize,equipment,extColor,exteriorColor,fuelType,highwayMpg,id,incentives,intColor,interiorColor,internetComments,internetPrice,inventoryDate,invoicePrice,key,location,make,mileage,model,modelCode,msrp,normalExteriorColor,normalFuelType,normalInteriorColor,numSaves,odometer,optionCodes,options,packageCode,paymentMonthly,payments,primary_image,saleLease,salePrice,status,stockNumber,transmission,trim,trimLevel,type,uuid,vin,year',
        },
        includePricing: true,
    };

    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            const proxyUrl = proxyConfig ? await proxyConfig.newUrl() : undefined;
            const response = await gotScraping({
                url,
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Origin': baseUrl,
                    'Referer': `${baseUrl}/new-inventory/`,
                },
                body: JSON.stringify(body),
                responseType: 'text',
                timeout: { request: 30000 },
                ...(proxyUrl ? { proxyUrl } : {}),
            });

            const statusCode = response.statusCode;
            if (statusCode === 429 || statusCode === 503) {
                const delay = 5000 * attempt;
                console.warn(`[WIS] HTTP ${statusCode} at ${url} (start=${startOffset}). Waiting ${delay}ms...`);
                await new Promise(r => setTimeout(r, delay));
                continue;
            }
            if (statusCode >= 400) {
                console.warn(`[WIS] HTTP ${statusCode} at ${url} (start=${startOffset}). Body: ${String(response.body).slice(0, 300)}`);
                if (attempt === retries) return null;
                await new Promise(r => setTimeout(r, 3000 * attempt));
                continue;
            }
            try {
                return JSON.parse(response.body);
            } catch (parseErr) {
                console.warn(`[WIS] JSON parse error (start=${startOffset}): ${parseErr.message}. Body: ${String(response.body).slice(0, 200)}`);
                if (attempt === retries) return null;
                await new Promise(r => setTimeout(r, 2000 * attempt));
                continue;
            }
        } catch (err) {
            if (attempt === retries) {
                console.error(`[WIS] All ${retries} attempts failed (start=${startOffset}): ${err.message}`);
                return null;
            }
            console.warn(`[WIS] Attempt ${attempt} failed (start=${startOffset}): ${err.message}. Retrying in ${2 * attempt}s...`);
            await new Promise(r => setTimeout(r, 2000 * attempt));
        }
    }
    return null;
}

// ─────────────────────────────────────────────────────────────────────────────
// DEALER CONFIGURATION
// ─────────────────────────────────────────────────────────────────────────────

const DEALERS = [
    {
        name: 'Freeland Chevrolet',
        platform: 'dealer_com',
        baseUrl: 'https://www.freelandchevrolet.com',
        siteId: 'freelandchevrolet',
        isOwnDealership: true,
    },
    {
        name: 'Carl Black Chevrolet Nashville',
        platform: 'algolia',
        baseUrl: 'https://www.carlblackchevy.com',
        isOwnDealership: false,
        algoliaAppId:    '1WNYBZLEEN',
        algoliaApiKey:   'e2acb682178e9dcc22d18ecb2ff7d9e4',
        algoliaIndex:    'carlblackchevynashville_production_inventory',
    },
    {
        name: 'Walker Chevrolet',
        platform: 'dealer_com',
        baseUrl: 'https://www.walkerchevrolet.com',
        siteId: 'walkerchevroletgm',
        isOwnDealership: false,
    },
    {
        name: 'Chevrolet Buick GMC of Murfreesboro',
        platform: 'dealer_com',
        baseUrl: 'https://www.chevroletbuickgmcofmurfreesboro.com',
        siteId: 'chevroletbuickgmcofmurfreesboro',
        isOwnDealership: false,
    },
    {
        name: 'Serra Chevrolet Buick GMC Nashville',
        platform: 'dealer_com',
        baseUrl: 'https://www.serranashville.com',
        siteId: 'serrachevroletbuickgmcnashville',
        isOwnDealership: false,
    },
    {
        name: 'Wilson County Chevrolet Buick GMC',
        platform: 'dealer_com',
        baseUrl: 'https://www.wilsoncountymotors.com',
        siteId: 'wilsoncountychevroletbuickgmc',
        isOwnDealership: false,
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
    const todayStr = today.toISOString().split('T')[0];

    for (const vehicle of vehicles) {
        if (!vehicle.vin) continue;

        const currentPrice = parseFloat(
            String(vehicle.dealerPrice || vehicle.msrp || '').replace(/[$,]/g, '')
        ) || null;

        if (vinHistory[vehicle.vin]) {
            const record = vinHistory[vehicle.vin];

            const firstSeen = new Date(record.firstSeenDate);
            const msOnLot = today - firstSeen;
            vehicle.firstSeenDate = record.firstSeenDate;
            // Use dealer's own daysOnLot if available (from DMS/WIS API) — it's more accurate
            // than our tracking which only counts days since first scrape
            const trackedDays = Math.floor(msOnLot / (1000 * 60 * 60 * 24));
            vehicle.daysOnLot = (vehicle.dealerDaysOnLot != null && vehicle.dealerDaysOnLot > 0)
                ? vehicle.dealerDaysOnLot
                : trackedDays;

            // Price history tracking
            if (currentPrice && record.priceHistory) {
                const lastPrice = record.priceHistory[record.priceHistory.length - 1]?.price;
                if (lastPrice !== currentPrice) {
                    record.priceHistory.push({ date: todayStr, price: currentPrice });
                }
                vehicle.priceHistory = record.priceHistory;
                vehicle.priceDropCount = record.priceHistory.filter((p, i) =>
                    i > 0 && p.price < record.priceHistory[i - 1].price
                ).length;
                vehicle.totalPriceDrop = record.priceHistory.length > 1
                    ? record.priceHistory[0].price - currentPrice
                    : 0;
            }

            // Update history record
            vinHistory[vehicle.vin] = {
                ...record,
                priceHistory: vehicle.priceHistory || record.priceHistory || [],
            };
        } else {
            // First time seeing this VIN
            vehicle.firstSeenDate = todayStr;
            // Use dealer's own daysOnLot if available, otherwise 0 (we just started tracking)
            vehicle.daysOnLot = (vehicle.dealerDaysOnLot != null && vehicle.dealerDaysOnLot > 0)
                ? vehicle.dealerDaysOnLot
                : 0;
            vehicle.priceHistory = currentPrice ? [{ date: todayStr, price: currentPrice }] : [];
            vehicle.priceDropCount = 0;
            vehicle.totalPriceDrop = 0;

            vinHistory[vehicle.vin] = {
                firstSeenDate: todayStr,
                dealer: vehicle.dealer,
                make: vehicle.make,
                model: vehicle.model,
                year: vehicle.year,
                trim: vehicle.trim,
                condition: vehicle.condition,
                priceHistory: vehicle.priceHistory,
            };
        }

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
            vinHistory[vin].soldDate = todayStr;
            vinHistory[vin].daysOnLot = daysOnLot;
        }
    }

    return soldVehicles;
}

// ─────────────────────────────────────────────────────────────────────────────
// DEALER.COM SCRAPER — Uses WIS POST endpoint (the real API the website uses)
// The WIS endpoint uses "start" parameter (array of strings) for pagination,
// unlike the legacy GET endpoint which uses "pageStart" but is CDN-cached.
// ─────────────────────────────────────────────────────────────────────────────
async function scrapeDealerCom(dealer, input) {
    const rawVehicles = [];
    const condition = (input.condition || 'all').toLowerCase();

    const conditions = condition === 'all' ? ['new', 'used'] : [condition];

    for (const cond of conditions) {
        await scrapeDealerComCondition(dealer, cond, input, rawVehicles);
        // Polite delay between new and used scrapes
        if (conditions.length > 1) await new Promise(r => setTimeout(r, 2000));
    }

    // Deduplicate by VIN
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

    console.log(`[${dealer.name}] Total: ${vehicles.length} unique vehicles`);
    return vehicles;
}

async function scrapeDealerComCondition(dealer, condition, input, vehicles) {
    let startOffset = 0;
    let totalCount = null;
    const pageSize = 100;
    const seenVins = new Set();

    console.log(`[${dealer.name}] Scraping ${condition.toUpperCase()} inventory via WIS POST...`);

    do {
        console.log(`[${dealer.name}] Fetching ${condition} vehicles starting at ${startOffset}...`);

        const data = await wisPostFetch(dealer.baseUrl, dealer.siteId, condition, startOffset, pageSize);

        if (!data) {
            console.error(`[${dealer.name}] No data returned for ${condition} at offset ${startOffset} — stopping`);
            break;
        }

        if (!data.pageInfo && !data.inventory) {
            console.error(`[${dealer.name}] Unexpected WIS response structure. Keys: ${Object.keys(data).join(', ')}`);
            break;
        }

        if (totalCount === null) {
            totalCount = data.pageInfo?.totalCount || 0;
            console.log(`[${dealer.name}] ${condition.toUpperCase()}: API reports ${totalCount} total vehicles`);
        }

        const pageVehicles = data.inventory || [];
        if (pageVehicles.length === 0) {
            console.log(`[${dealer.name}] Empty page at offset ${startOffset} — stopping`);
            break;
        }

        // Check for page loop (same VINs as before)
        const newVins = pageVehicles.map(v => v.vin).filter(v => v && !seenVins.has(v));
        if (newVins.length === 0 && startOffset > 0) {
            console.log(`[${dealer.name}] All ${pageVehicles.length} vehicles on this page already seen — stopping pagination`);
            break;
        }

        for (const v of pageVehicles) {
            if (v.vin) seenVins.add(v.vin);
            const vehicle = parseDealerComVehicle(v, dealer, condition);
            if (shouldInclude(vehicle, input)) {
                vehicles.push(vehicle);
            }
        }

        console.log(`[${dealer.name}] Got ${pageVehicles.length} vehicles (${newVins.length} new). Running total: ${vehicles.length}`);

        startOffset += pageSize;

        // Polite delay between pages
        if (startOffset < totalCount) await new Promise(r => setTimeout(r, 800));

    } while (startOffset < totalCount);

    console.log(`[${dealer.name}] ${condition.toUpperCase()} scrape complete: ${seenVins.size} unique VINs`);
}

function parseDealerComVehicle(v, dealer, conditionOverride) {
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
    // v.link is often empty for WIS dealers; fall back to VIN-filtered search URL
    const detailPath = v.link || '';
    let detailUrl;
    if (detailPath.startsWith('http')) {
        detailUrl = detailPath;
    } else if (detailPath && detailPath !== '/') {
        detailUrl = `${dealer.baseUrl}${detailPath}`;
    } else if (v.vin) {
        // Use VIN-filtered inventory search URL — works on all Dealer.com sites
        const conditionSlug = (conditionOverride || v.condition || 'new').toLowerCase() === 'used' ? 'used-inventory' : 'new-inventory';
        detailUrl = `${dealer.baseUrl}/${conditionSlug}/index.htm?vin=${v.vin}`;
    } else {
        detailUrl = dealer.baseUrl;
    }

    // Primary photo
    const images = v.images || [];
    const primaryPhoto = images.length > 0 ? images[0].uri : null;

    return {
        dealer: dealer.name,
        isOwnDealership: dealer.isOwnDealership || false,
        platform: 'Dealer.com',
        accountId: v.accountId || null,
        condition: conditionOverride || v.condition || null,
        year: v.year || null,
        make: v.make || null,
        model: v.model || null,
        trim: v.trim || null,
        bodyStyle: v.bodyStyle || null,
        fuelType: v.fuelType || null,
        vin: v.vin || null,
        stockNumber: v.stockNumber || null,
        status: parseStatus(v.status),
        exteriorColor: attrs.exteriorColor || attrs.normalExteriorColor || null,
        interiorColor: attrs.interiorColor || attrs.normalInteriorColor || null,
        engine: attrs.engine || null,
        transmission: attrs.transmission || null,
        drivetrain: attrs.normalDriveLine || attrs.driveLine || null,
        mileage: attrs.odometer || null,
        mpgCity: attrs.cityMpg || null,
        mpgHighway: attrs.highwayMpg || null,
        msrp: msrp,
        dealerPrice: dealerPrice || finalPrice,
        primaryPhotoUrl: primaryPhoto,
        photoCount: images.length,
        detailUrl: detailUrl,
        scrapedAt: new Date().toISOString(),
        firstSeenDate: null,
        // dealerDaysOnLot: the dealer's own reported days-on-lot from WIS API
        // This is the authoritative value from the dealer's DMS system
        dealerDaysOnLot: attrs.daysOnLot != null ? parseInt(attrs.daysOnLot, 10) || null : null,
        daysOnLot: null,
        ageBucket: null,
    };
}

function parseStatus(statusCode) {
    const statusMap = { 1: 'On Lot', 7: 'In Transit', 2: 'On Order' };
    return statusMap[statusCode] || `Unknown (${statusCode})`;
}

// ─────────────────────────────────────────────────────────────────────────────
// ALGOLIA SCRAPER (Carl Black Chevrolet)
// Carl Black uses Dealer Inspire + Algolia search
// ─────────────────────────────────────────────────────────────────────────────
async function scrapeAlgolia(dealer, input) {
    const { algoliaAppId, algoliaApiKey, algoliaIndex } = dealer;
    const condition = (input.condition || 'all').toLowerCase();
    const pageSize = 100;
    let page = 0;
    const vehicles = [];
    let totalPages = 1;

    console.log(`[${dealer.name}] Starting Algolia scrape (index: ${algoliaIndex})...`);

    do {
        const body = {
            requests: [{
                indexName: algoliaIndex,
                params: new URLSearchParams({
                    hitsPerPage: pageSize,
                    page,
                    ...(condition !== 'all' ? { filters: `type:${condition}` } : {}),
                }).toString(),
            }],
        };

        try {
            const response = await fetchWithRetry(
                `https://${algoliaAppId}-dsn.algolia.net/1/indexes/*/queries`,
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'X-Algolia-Application-Id': algoliaAppId,
                        'X-Algolia-API-Key': algoliaApiKey,
                    },
                    body: JSON.stringify(body),
                }
            );
            const data = await response.json();
            const result = data.results?.[0];
            if (!result) break;

            totalPages = result.nbPages || 1;
            console.log(`[${dealer.name}] Algolia page ${page + 1}/${totalPages}: ${result.hits.length} vehicles`);

            for (const hit of result.hits) {
                const vehicle = parseAlgoliaVehicle(hit, dealer);
                if (shouldInclude(vehicle, input)) {
                    vehicles.push(vehicle);
                }
            }

            page++;
        } catch (err) {
            console.error(`[${dealer.name}] Algolia error on page ${page}: ${err.message}`);
            break;
        }
    } while (page < totalPages);

    console.log(`[${dealer.name}] Algolia scrape complete: ${vehicles.length} vehicles`);
    return vehicles;
}

function parseAlgoliaVehicle(hit, dealer) {
    return {
        dealer: dealer.name,
        isOwnDealership: dealer.isOwnDealership || false,
        platform: 'Dealer Inspire (Algolia)',
        accountId: null,
        condition: hit.type || null,
        year: hit.year ? parseInt(hit.year) : null,
        make: hit.make || null,
        model: hit.model || null,
        trim: hit.trim || null,
        bodyStyle: hit.body_style || null,
        fuelType: hit.fuel_type || null,
        vin: hit.vin || null,
        stockNumber: hit.stock_number || null,
        status: hit.status || 'On Lot',
        exteriorColor: hit.ext_color_generic || hit.ext_color || null,
        interiorColor: hit.int_color_generic || hit.int_color || null,
        engine: hit.engine || null,
        transmission: hit.transmission || null,
        drivetrain: hit.drivetrain || null,
        mileage: hit.miles ? String(hit.miles) : null,
        mpgCity: hit.city_mpg ? String(hit.city_mpg) : null,
        mpgHighway: hit.highway_mpg ? String(hit.highway_mpg) : null,
        msrp: hit.msrp || null,
        dealerPrice: hit.our_price || hit.msrp || null,
        primaryPhotoUrl: hit.thumbnail || null,
        photoCount: hit.photo_count || 0,
        detailUrl: hit.link ? (hit.link.startsWith('http') ? hit.link : `${dealer.baseUrl}${hit.link}`) : null,
        scrapedAt: new Date().toISOString(),
        firstSeenDate: null,
        // Algolia may include days_on_lot or age field
        dealerDaysOnLot: hit.days_on_lot != null ? parseInt(hit.days_on_lot, 10) || null
            : hit.age != null ? parseInt(hit.age, 10) || null : null,
        daysOnLot: null,
        ageBucket: null,
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// FILTER HELPER
// ─────────────────────────────────────────────────────────────────────────────
function shouldInclude(vehicle, input) {
    if (input.make && vehicle.make?.toLowerCase() !== input.make.toLowerCase()) return false;
    if (input.model && !vehicle.model?.toLowerCase().includes(input.model.toLowerCase())) return false;
    if (input.minYear && vehicle.year < input.minYear) return false;
    if (input.maxYear && vehicle.year > input.maxYear) return false;
    return true;
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN ENTRY POINT
// ─────────────────────────────────────────────────────────────────────────────
Actor.main(async () => {
    const input = await Actor.getInput() || {};
    const condition = input.condition || 'all';
    const dealerFilter = input.dealer || null;

    console.log('=== Freeland Dealer Intelligence Scraper ===');
    console.log(`Condition: ${condition} | Dealer filter: ${dealerFilter || 'all'}`);

    // Initialize proxy configuration for IP rotation
    try {
        proxyConfig = await Actor.createProxyConfiguration({
            groups: ['RESIDENTIAL'],
            countryCode: 'US',
        });
        console.log('Proxy configuration initialized: RESIDENTIAL US proxies enabled');
    } catch (err) {
        console.warn(`Proxy configuration failed (may need Apify Scale plan): ${err.message}`);
        console.warn('Continuing without proxy rotation — pagination may be limited by CDN rate limiting');
        proxyConfig = null;
    }

    // Initialize KV store for VIN history tracking
    const kvStore = await Actor.openKeyValueStore('inventory-tracker');
    const vinHistory = await loadVinHistory(kvStore);
    console.log(`Loaded VIN history: ${Object.keys(vinHistory).length} tracked VINs`);

    // Open the named inventory dataset
    // We keep the same named dataset across runs (no drop/delete) to preserve the dataset ID.
    // The dashboard deduplicates by VIN keeping the latest record, so accumulation is safe.
    // Each run pushes fresh data; the dashboard always picks the most recent record per VIN.
    const inventoryDs = await Actor.openDataset('inventory');
    console.log('Opened inventory dataset for this run');

    const soldDataset = await Actor.openDataset('sold-vehicles');

    const today = new Date();
    const allVehicles = [];
    const dealerSummary = [];

    const dealersToScrape = dealerFilter
        ? DEALERS.filter(d => d.name.toLowerCase().includes(dealerFilter.toLowerCase()))
        : DEALERS;

    for (const dealer of dealersToScrape) {
        console.log(`\n--- Scraping ${dealer.name} (${dealer.platform}) ---`);
        const startTime = Date.now();

        let vehicles = [];
        try {
            if (dealer.platform === 'dealer_com') {
                vehicles = await scrapeDealerCom(dealer, { condition });
            } else if (dealer.platform === 'algolia') {
                vehicles = await scrapeAlgolia(dealer, { condition });
            } else {
                console.warn(`[${dealer.name}] Unknown platform: ${dealer.platform}`);
                continue;
            }
        } catch (err) {
            console.error(`[${dealer.name}] Scrape failed: ${err.message}`);
            dealerSummary.push({ dealer: dealer.name, count: 0, error: err.message });
            continue;
        }

        // Apply days-in-stock tracking
        vehicles = applyDaysInStock(vehicles, vinHistory, today);

        // Save incrementally — push each dealer's data immediately so a timeout doesn't lose it
        if (vehicles.length > 0) {
            await inventoryDs.pushData(vehicles);
            console.log(`[${dealer.name}] Saved ${vehicles.length} vehicles to dataset`);
        }

        allVehicles.push(...vehicles);
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
        dealerSummary.push({ dealer: dealer.name, count: vehicles.length, elapsed: `${elapsed}s` });
        console.log(`[${dealer.name}] Done: ${vehicles.length} vehicles in ${elapsed}s`);

        // Polite delay between dealers
        if (dealersToScrape.indexOf(dealer) < dealersToScrape.length - 1) {
            await new Promise(r => setTimeout(r, 3000));
        }
    }

    // All vehicles already saved incrementally above
    console.log(`\nTotal vehicles saved: ${allVehicles.length}`);

    // Detect and save sold vehicles
    const currentVins = new Set(allVehicles.map(v => v.vin).filter(Boolean));
    const soldVehicles = detectSoldVehicles(vinHistory, currentVins, today);
    if (soldVehicles.length > 0) {
        await soldDataset.pushData(soldVehicles);
        console.log(`Detected ${soldVehicles.length} sold/removed vehicles`);
    }

    // Save updated VIN history
    await saveVinHistory(kvStore, vinHistory);

    // Print summary table
    console.log('\n=== RUN SUMMARY ===');
    console.log('Dealer'.padEnd(45) + 'Vehicles'.padEnd(12) + 'Time');
    console.log('-'.repeat(65));
    for (const s of dealerSummary) {
        const status = s.error ? `ERROR: ${s.error.slice(0, 30)}` : s.elapsed;
        console.log(s.dealer.padEnd(45) + String(s.count).padEnd(12) + status);
    }
    console.log('-'.repeat(65));
    console.log('TOTAL'.padEnd(45) + String(allVehicles.length));
});
