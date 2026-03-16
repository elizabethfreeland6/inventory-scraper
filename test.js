// Standalone test — validates the core scraping logic against live dealer APIs
// Run with: node test.js

import { gotScraping } from 'got-scraping';

const DEALERS_DEALERCOM = [
    {
        name: 'Walker Chevrolet',
        apiUrl: 'https://www.walkerchevrolet.com/apis/widget/INVENTORY_LISTING_DEFAULT_AUTO_ALL:inventory-data-bus1/getInventory',
        baseUrl: 'https://www.walkerchevrolet.com',
    },
    {
        name: 'Serra Chevrolet Buick GMC Nashville',
        apiUrl: 'https://www.serranashville.com/apis/widget/INVENTORY_LISTING_DEFAULT_AUTO_ALL:inventory-data-bus1/getInventory',
        baseUrl: 'https://www.serranashville.com',
    },
    {
        name: 'Chevrolet Buick GMC of Murfreesboro',
        apiUrl: 'https://www.chevroletbuickgmcofmurfreesboro.com/apis/widget/INVENTORY_LISTING_DEFAULT_AUTO_ALL:inventory-data-bus1/getInventory',
        baseUrl: 'https://www.chevroletbuickgmcofmurfreesboro.com',
    },
    {
        name: 'Darrell Waltrip Buick GMC',
        apiUrl: 'https://www.darrellwaltripbuickgmc.com/apis/widget/INVENTORY_LISTING_DEFAULT_AUTO_ALL:inventory-data-bus1/getInventory',
        baseUrl: 'https://www.darrellwaltripbuickgmc.com',
    },
];

const HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/html, */*',
    'Accept-Language': 'en-US,en;q=0.9',
};

function parseDealerComVehicle(v, dealer) {
    const attrs = {};
    for (const attr of (v.attributes || [])) {
        attrs[attr.name] = attr.value;
    }
    const pricing = v.pricing || {};
    const msrp = pricing.retailPrice || null;
    let dealerPrice = null;
    for (const dp of (pricing.dprice || [])) {
        if (dp.isFinalPrice || dp.typeClass === 'wholesalePrice') dealerPrice = dp.value;
    }
    const detailPath = v.link || '';
    const detailUrl = detailPath.startsWith('http') ? detailPath : `${dealer.baseUrl}${detailPath}`;
    const images = v.images || [];
    const statusMap = { 1: 'On Lot', 7: 'In Transit', 2: 'On Order' };

    return {
        dealer: dealer.name,
        condition: v.condition || null,
        year: v.year || null,
        make: v.make || null,
        model: v.model || null,
        trim: v.trim || null,
        bodyStyle: v.bodyStyle || null,
        fuelType: v.fuelType || null,
        vin: v.vin || null,
        stockNumber: v.stockNumber || null,
        status: statusMap[v.status] || `Unknown(${v.status})`,
        exteriorColor: attrs.exteriorColor || null,
        interiorColor: attrs.interiorColor || null,
        engine: attrs.engine || null,
        transmission: attrs.transmission || null,
        drivetrain: attrs.normalDriveLine || null,
        mileage: attrs.odometer || null,
        msrp: msrp,
        dealerPrice: dealerPrice,
        primaryPhotoUrl: images.length > 0 ? images[0].uri : null,
        detailUrl: detailUrl,
    };
}

async function testDealer(dealer) {
    const url = `${dealer.apiUrl}?pageSize=5&pageStart=0`;
    try {
        const response = await gotScraping({
            url,
            headers: HEADERS,
            responseType: 'json',
            timeout: { request: 15000 },
        });
        const data = response.body;
        const total = data.pageInfo?.totalCount || 0;
        const pageVehicles = (data.inventory || []).slice(0, 3);
        const parsed = pageVehicles.map(v => parseDealerComVehicle(v, dealer));

        console.log(`\n✅ ${dealer.name}`);
        console.log(`   Total inventory: ${total}`);
        console.log(`   Sample vehicles:`);
        for (const v of parsed) {
            console.log(`   - ${v.year} ${v.make} ${v.model} ${v.trim} | ${v.condition} | ${v.status} | MSRP: ${v.msrp} | Dealer: ${v.dealerPrice} | Color: ${v.exteriorColor} | VIN: ${v.vin}`);
        }
        return { dealer: dealer.name, total, sample: parsed };
    } catch (err) {
        console.log(`\n❌ ${dealer.name}: ${err.message}`);
        return { dealer: dealer.name, error: err.message };
    }
}

console.log('Testing Dealer.com API endpoints...\n');
const results = await Promise.all(DEALERS_DEALERCOM.map(testDealer));

console.log('\n\n=== SUMMARY ===');
for (const r of results) {
    if (r.error) {
        console.log(`❌ ${r.dealer}: FAILED - ${r.error}`);
    } else {
        console.log(`✅ ${r.dealer}: ${r.total} vehicles`);
    }
}
