# Assets

## Hero background image

Save a finance-themed image as **`assets/hero_bg.jpg`** (or `.jpeg` / `.png`)
and the PremiumEdge dashboard header will use it automatically — a dark
left-to-right gradient is layered on top so the title stays readable, so
images that are busiest on the right side work best.

The image is downscaled once to ≤1800px wide (cached as
`.hero_bg_optimized.jpg`) to keep page loads fast. Delete the cache file after
replacing the image if it doesn't refresh.

Without an image, the built-in SVG candlestick scene is used.

## Controls-zone background image

The Run/profile/Configure area uses `config/Wall Street Bull Image.png` if it
exists, else `assets/controls_bg.jpg` / `.png`, else no background. Same
optimization/caching as the hero (`.controls_bg_optimized.jpg`).
