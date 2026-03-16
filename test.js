# Nashville Chevrolet Dealer Inventory Scraper

Scrapes vehicle inventory from **5 Nashville-area Chevrolet dealerships** and returns clean, structured data for each vehicle. Built to work with Claude via Apify's MCP connector.

## Dealers Covered

| Dealer | Platform | Website |
|---|---|---|
| Carl Black Chevrolet Nashville | Dealer Inspire | carlblackchevy.com |
| Walker Chevrolet | Dealer.com | walkerchevrolet.com |
| Chevrolet Buick GMC of Murfreesboro | Dealer.com | chevroletbuickgmcofmurfreesboro.com |
| Serra Chevrolet Buick GMC Nashville | Dealer.com | serranashville.com |
| Darrell Waltrip Buick GMC | Dealer.com | darrellwaltripbuickgmc.com |

## Output Fields

Each vehicle record contains:

| Field | Description |
|---|---|
| `dealer` | Dealership name |
| `platform` | Website platform (Dealer.com or Dealer Inspire) |
| `condition` | New or Used |
| `year` | Model year |
| `make` | Make (Chevrolet, GMC, Buick, etc.) |
| `model` | Model name |
| `trim` | Trim level |
| `bodyStyle` | SUV, Truck, Sedan, etc. |
| `fuelType` | Gasoline, Electric, Hybrid |
| `vin` | Vehicle Identification Number |
| `stockNumber` | Dealer stock number |
| `status` | On Lot / In Transit / On Order |
| `exteriorColor` | Exterior color name |
| `interiorColor` | Interior color name |
| `engine` | Engine description |
| `transmission` | Transmission type |
| `drivetrain` | FWD / RWD / AWD / 4WD |
| `mileage` | Odometer reading |
| `mpgCity` | City fuel economy |
| `mpgHighway` | Highway fuel economy |
| `msrp` | MSRP price |
| `dealerPrice` | Dealer's asking price (after discounts) |
| `primaryPhotoUrl` | URL to the primary vehicle photo |
| `photoCount` | Total number of photos |
| `detailUrl` | Link to the vehicle detail page |
| `scrapedAt` | Timestamp when the data was collected |

## Input Options

| Input | Type | Default | Description |
|---|---|---|---|
| `dealers` | string[] | all | Filter to specific dealers by name keyword |
| `condition` | string | `new` | `new`, `used`, or `all` |
| `make` | string | any | Filter by make (e.g. `Chevrolet`) |
| `model` | string | any | Filter by model keyword (e.g. `Silverado`) |
| `minYear` | integer | none | Minimum model year |
| `maxYear` | integer | none | Maximum model year |
| `minPrice` | integer | none | Minimum price in dollars |
| `maxPrice` | integer | none | Maximum price in dollars |

## Example Input (JSON)

```json
{
    "condition": "new",
    "make": "Chevrolet",
    "model": "Silverado",
    "minYear": 2025
}
```

## Using with Claude via Apify MCP

Once this Actor is published to your Apify account, you can connect it to Claude using the Apify MCP server URL: `https://mcp.apify.com`

Then ask Claude things like:
- *"Run my dealer inventory scraper and show me all new Silverado 1500s across all dealers"*
- *"Compare Tahoe pricing across Walker and Serra"*
- *"Which dealer has the most in-transit Silverado HDs?"*

## Notes

- **Dealer.com** dealers are scraped via an internal JSON API — fast and reliable, returns all vehicles in paginated batches.
- **Dealer Inspire** (Carl Black) is scraped via HTML parsing with JSON-LD structured data as a fallback.
- Scraping is done respectfully with standard browser headers and no aggressive rate limiting.
- Data is publicly available on each dealer's website and is intended for competitive market research.
