# ── INTEL Pre-Foreclosure Data Fetcher ───────────────────────────────────────
# Compatible with Windows PowerShell 5.1
# Usage: .\fetch_preforeclosure.ps1

$API_KEY = "sr8Rlql1aGZHeEdVW9zVjnebOTT8vF9mUQAheao6"
$API_URL = "https://api.batchdata.com/api/v1/property/search"

$COUNTIES = @(
    @{ query = "Sedgwick County, KS"; key = "sedgwick"; city = "Wichita";   state = "KS" },
    @{ query = "Harris County, TX";   key = "harris";   city = "Houston";   state = "TX" },
    @{ query = "Clark County, NV";    key = "clark";    city = "Las Vegas"; state = "NV" }
)

function Coalesce($a, $b) {
    if ($null -ne $a -and $a -ne "") { return $a }
    return $b
}

function Make-Id($parts) {
    $str = ($parts -join "|").ToLower()
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($str)
    $md5 = [System.Security.Cryptography.MD5]::Create()
    $hash = $md5.ComputeHash($bytes)
    return [System.BitConverter]::ToString($hash).Replace("-","").ToLower().Substring(0,16)
}

function Format-Money($val) {
    if ($null -eq $val -or $val -eq 0) { return $null }
    return "`$" + ("{0:N2}" -f $val)
}

function Parse-Property($prop, $countyKey, $city, $state) {
    $addr    = if ($prop.address)     { $prop.address }     else { $null }
    $owner   = if ($prop.owner)       { $prop.owner }       else { $null }
    $fc      = if ($prop.foreclosure) { $prop.foreclosure } else { $null }
    $val     = if ($prop.valuation)   { $prop.valuation }   else { $null }
    $listing = if ($prop.listing)     { $prop.listing }     else { $null }
    $openLien= if ($prop.openLien)    { $prop.openLien }    else { $null }
    $intel   = if ($prop.intel)       { $prop.intel }       else { $null }

    $street    = if ($addr) { $addr.street } else { "" }
    $addrCity  = if ($addr -and $addr.city)  { $addr.city }  else { $city }
    $addrState = if ($addr -and $addr.state) { $addr.state } else { $state }
    $fullAddr  = "$street, $addrCity $addrState"

    $ownerName = if ($owner -and $owner.fullName) { $owner.fullName } else { "SEE COUNTY RECORDS" }

    $amount = $null
    if ($fc -and $fc.auctionMinimumBidAmount) { $amount = Format-Money $fc.auctionMinimumBidAmount }
    elseif ($openLien -and $openLien.totalOpenLienBalance) { $amount = Format-Money $openLien.totalOpenLienBalance }

    $auctionDate = if ($fc -and $fc.auctionDate) { $fc.auctionDate } else { "" }
    if ($auctionDate) {
        try { $auctionDate = ([datetime]$auctionDate).ToString("yyyy-MM-dd") } catch {}
    }

    $filingDate = ""
    if ($fc -and $fc.filingDate)    { $filingDate = $fc.filingDate }
    elseif ($fc -and $fc.recordingDate) { $filingDate = $fc.recordingDate }

    $caseNumber = if ($fc -and $fc.caseNumber) { $fc.caseNumber } else { "" }
    $lender     = if ($fc -and $fc.currentLenderName) { $fc.currentLenderName } else { "" }
    $propType   = if ($listing -and $listing.propertyType) { $listing.propertyType } else { "" }
    $bedrooms   = if ($listing -and $listing.bedroomCount) { $listing.bedroomCount } else { $null }
    $estValue   = if ($val -and $val.estimatedValue) { $val.estimatedValue } else { $null }

    $notesParts = @()
    if ($fc -and $fc.status)          { $notesParts += $fc.status }
    if ($auctionDate)                 { $notesParts += "Auction: $auctionDate" }
    if ($fc -and $fc.auctionLocation) { $notesParts += "@ $($fc.auctionLocation), $($fc.auctionCity)" }
    if ($fc -and $fc.trusteeName)     { $notesParts += "Trustee: $($fc.trusteeName)" }
    if ($caseNumber)                  { $notesParts += "Case: $caseNumber" }
    if ($estValue)                    { $notesParts += ("Est. Value: `$" + $estValue.ToString('N0')) }

    $score = 50
    if ($intel -and $intel.salePropensity) {
        $sp = [int]$intel.salePropensity
        if ($sp -gt 100) { $sp = 100 }
        $score = $sp
    }

    return [ordered]@{
        id             = Make-Id @($countyKey, "preforeclosure", $street, $ownerName)
        county         = $countyKey
        type           = "pre-foreclosure"
        owner          = $ownerName
        address        = $fullAddr
        amount         = $amount
        filingDate     = $filingDate
        caseNumber     = $caseNumber
        phone          = $null
        score          = $score
        scrapedAt      = (Get-Date).ToUniversalTime().ToString("o")
        propertyType   = $propType
        bedrooms       = $bedrooms
        estimatedValue = $estValue
        auctionDate    = $auctionDate
        lender         = $lender
        notes          = ($notesParts -join " | ")
        source         = "BatchData Pre-Foreclosure"
    }
}

function Fetch-County($county) {
    $key   = $county.key
    $query = $county.query
    Write-Host "`n=================================================="
    Write-Host "  $($query.ToUpper())"
    Write-Host "=================================================="

    $allLeads = @{}
    $skip = 0
    $pageSize = 100
    $totalFound = 0

    do {
        $payload = @{
            searchCriteria = @{
                query      = $query
                quickLists = @("preforeclosure")
            }
            options = @{
                take = $pageSize
                skip = $skip
            }
        } | ConvertTo-Json -Depth 5

        try {
            $response = Invoke-WebRequest -UseBasicParsing `
                -Uri $API_URL `
                -Method POST `
                -Headers @{
                    "Authorization" = "Bearer $API_KEY"
                    "Content-Type"  = "application/json"
                } `
                -Body $payload

            $data       = $response.Content | ConvertFrom-Json
            $props      = @($data.results.properties)
            $totalFound = $data.meta.results.resultsFound

            Write-Host "  Page skip=$skip : $($props.Count) results (total: $totalFound)"

            foreach ($prop in $props) {
                $lead = Parse-Property $prop $key $county.city $county.state
                $allLeads[$lead.id] = $lead
            }

            $skip += $pageSize

        } catch {
            Write-Host "  ERROR: $_" -ForegroundColor Red
            break
        }

    } while ($skip -lt $totalFound -and $props.Count -eq $pageSize)

    # Sort by score descending
    $leadsList = @($allLeads.Values | Sort-Object { $_.score } -Descending)

    $output = [ordered]@{
        lastUpdated = (Get-Date).ToUniversalTime().ToString("o")
        totalLeads  = $leadsList.Count
        totalFound  = $totalFound
        source      = "BatchData"
        leads       = $leadsList
    }

    # Save to data/ folder
    if (-not (Test-Path "data")) {
        New-Item -ItemType Directory -Path "data" | Out-Null
    }
    $outPath = "data\leads-$key-preforeclosure.json"
    $output | ConvertTo-Json -Depth 10 | Out-File -FilePath $outPath -Encoding utf8
    Write-Host "  Saved $($leadsList.Count) leads -> $outPath" -ForegroundColor Green
}

# ── Main ──────────────────────────────────────────────────────────────────────
Write-Host "============================================================"
Write-Host "INTEL Pre-Foreclosure Fetcher"
Write-Host (Get-Date).ToString()
Write-Host "============================================================"

foreach ($county in $COUNTIES) {
    Fetch-County $county
}

Write-Host "`n============================================================"
Write-Host "DONE! Now commit and push the data/ folder to GitHub."
Write-Host "============================================================"
