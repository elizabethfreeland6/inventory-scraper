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
//   • Dealer.com platform  (Walker, Murfreesboro, Serra, Darrell Waltrip)
//   • Dealer Inspire platform (Carl Black)
// ─────────────────────────────────────────────────────────────────────────────

const DEALERS = [
    {
        name: 'Carl Black Chevrolet Nashville',
        platform: 'dealer_inspire',
        baseUrl: 'https://www.carlblackchevy.com',
        inventoryUrl: 'https://www.carlblackchevy.com/new-vehicles/',
    },
    {
        name: 'Walker Chevrolet',
        platform: 'dealer_com',
        baseUrl: 'https://www.walkerchevrolet.com',
        apiUrl: 'https://www.walkerchevrolet.com/apis/widget/INVENTORY_LISTING_DEFAULT_AUTO_ALL:inventory-data-bus1/getInventory',
    },
    {
        name: 'Chevrolet Buick GMC of Murfreesboro',
        platform: 'dealer_com',
        baseUrl: 'https://www.chevroletbuickgmcofmurfreesboro.com',
        apiUrl: 'https://www.chevroletbuickgmcofmurfreesboro.com/apis/widget/INVENTORY_LISTING_DEFAULT_AUTO_ALL:inventory-data-bus1/getInventory',
    },
    {
        name: 'Serra Chevrolet Buick GMC Nashville',
        platform: 'dealer_com',
        baseUrl: 'https://www.serranashville.com',
        apiUrl: 'https://www.serranashville.com/apis/widget/INVENTORY_LISTING_DEFAULT_AUTO_ALL:inventory-data-bus1/getInventory',
    },
    {
        name: 'Darrell Waltrip Buick GMC',
        platform: 'dealer_com',
        baseUrl: 'https://www.darrellwaltripbuickgmc.com',
        // Note: This dealer has ~6,000 used vehicles in their ALL endpoint (likely a dealer group
        // aggregator). Using separate NEW and USED endpoints to allow condition-based filtering.
        apiUrlNew: 'https://www.darrellwaltripbuickgmc.com/apis/widget/INVENTORY_LISTING_DEFAULT_AUTO_NEW:inventory-data-bus1/getInventory',
        apiUrlUsed: 'https://www.darrellwaltripbuickgmc.com/apis/widget/INVENTORY_LISTING_DEFAULT_AUTO_USED:inventory-data-bus1/getInventory',
        apiUrl: 'https://www.darrellwaltripbuickgmc.com/apis/widget/INVENTORY_LISTING_DEFAULT_AUTO_NEW:inventory-data-bus1/getInventory',
    },
];

const HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/html, */*',
    'Accept-Language': 'en-US,en;q=0.9',
};

// ─────────────────────────────────────────────────────────────────────────────
// DEALER.COM SCRAPER
// Uses the undocumented but publicly accessible JSON API endpoint.
// Paginates through all results using pageStart parameter.
// ─────────────────────────────────────────────────────────────────────────────
async function scrapeDealerCom(dealer, input) {
    const vehicles = [];

    // Darrell Waltrip has separate new/used endpoints to avoid pulling 6,000+ aggregated used vehicles
    // Determine which endpoint(s) to use based on the condition filter
    let endpoints = [];
    if (dealer.apiUrlNew && dealer.apiUrlUsed) {
        const condition = (input.condition || 'new').toLowerCase();
        if (condition === 'new') endpoints = [dealer.apiUrlNew];
        else if (condition === 'used') endpoints = [dealer.apiUrlUsed];
        else endpoints = [dealer.apiUrlNew, dealer.apiUrlUsed]; // 'all'
    } else {
        endpoints = [dealer.apiUrl];
    }

    for (const apiUrl of endpoints) {
        await scrapeDealerComEndpoint(dealer, apiUrl, input, vehicles);
    }

    console.log(`[${dealer.name}] Total after all endpoints: ${vehicles.length} vehicles`);
    return vehicles;
}

async function scrapeDealerComEndpoint(dealer, apiUrl, input, vehicles) {
    let pageStart = 0;
    let totalCount = null;
    const pageSize = 100; // Request max per page to minimize requests

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
            console.log(`[${dealer.name}] Total vehicles: ${totalCount}`);
        }

        const pageVehicles = data.inventory || [];
        for (const v of pageVehicles) {
            const vehicle = parseDealerComVehicle(v, dealer);
            // Apply filters from input
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
        platform: 'Dealer.com',
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
    };
}

function parseStatus(statusCode) {
    // Dealer.com status codes: 1 = On Lot, 7 = In Transit
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

console.log(`Scraping ${targetDealers.length} dealer(s)...`);
console.log(`Filters: condition=${input.condition || 'all'}, make=${input.make || 'any'}, model=${input.model || 'any'}`);

const dataset = await Actor.openDataset();
let totalSaved = 0;

for (const dealer of targetDealers) {
    let vehicles = [];

    try {
        if (dealer.platform === 'dealer_com') {
            vehicles = await scrapeDealerCom(dealer, input);
        } else if (dealer.platform === 'dealer_inspire') {
            vehicles = await scrapeDealerInspire(dealer, input);
        }
    } catch (err) {
        console.error(`[${dealer.name}] Fatal error: ${err.message}`);
    }

    if (vehicles.length > 0) {
        await dataset.pushData(vehicles);
        totalSaved += vehicles.length;
        console.log(`[${dealer.name}] Saved ${vehicles.length} vehicles to dataset`);
    }
}

console.log(`\nDone! Total vehicles saved: ${totalSaved}`);

await Actor.exit();
