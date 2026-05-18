# cap-protocol.org — official site

Static single-page site for the CAP (CLI Agent Protocol)
specification. No framework, no build step.

## Files

| File | Purpose |
|---|---|
| `index.html` | Sole HTML entry. |
| `style.css`  | All styles. CSS vars at top for easy theming. |
| `favicon.svg` | Brand mark. |

## Local preview

```bash
cd website
python3 -m http.server 8000
# open http://localhost:8000
```

Any static server works. No JS dependencies; no compilation.

## Deployment

### Cloudflare Pages (recommended)

1. Push repo to GitHub.
2. Cloudflare Pages → connect repo.
3. Build settings:
   - Build command: *(none)*
   - Build output directory: `website`
4. DNS:
   - `cap-protocol.org` → CNAME to `<project>.pages.dev`
   - `www.cap-protocol.org` → same

### GitHub Pages

1. Repo settings → Pages → Source: `main` branch, `/website` folder.
2. Custom domain: `cap-protocol.org`.
3. Add `CNAME` file containing `cap-protocol.org` to the `website/`
   directory before enabling.

### S3 / R2 / any static host

Just upload all three files to the bucket root. Set
`index.html` as the default document.

## Editing checklist

When editing the site, keep in mind:

- The spec is the source of truth, the site is a pointer.
- Don't duplicate spec content; link to it.
- Status badges in `index.html` `<section class="profiles">`
  reflect spec reality — update both together.
- The "Status" timeline at `<section class="status">` should be
  updated on every milestone.
- Keep the page single-screen-deep on first load: hero +
  pitch + diagram. Anything past that is for the curious.

## License

Page content is under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/),
matching the spec.
