import { fileURLToPath } from "node:url";
import { defineConfig } from "astro/config";

// https://astro.build/config
export default defineConfig({
  site: "https://abbas-hoseiny.github.io",
  base: "/pflanzenschutzliste",
  output: "static",
  build: {
    inlineStylesheets: "auto",
    assets: "_astro",
  },
  vite: {
    resolve: {
      alias: {
        "@": fileURLToPath(new URL("./src", import.meta.url)),
        "@scripts": fileURLToPath(new URL("./src/scripts", import.meta.url)),
        "@components": fileURLToPath(
          new URL("./src/components", import.meta.url)
        ),
        "@styles": fileURLToPath(new URL("./src/styles", import.meta.url)),
        "@config": fileURLToPath(new URL("./src/config", import.meta.url)),
      },
    },
    build: {
      rollupOptions: {
        output: {
          manualChunks: {
            sqlite: ["./src/scripts/core/storage/sqlite"],
            bvl: [
              "./src/scripts/core/bvlSync",
              "./src/scripts/core/bvlDataset",
              "./src/scripts/core/bvlClient",
            ],
            vendor: ["./src/scripts/core/bootstrap"],
          },
        },
      },
    },
  },
});
