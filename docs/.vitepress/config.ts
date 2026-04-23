import { defineConfig } from "vitepress";

// https://vitepress.dev/reference/site-config
export default defineConfig({
  title: "datannur builder docs",
  description: "Build and export datannur catalogs from files and databases",
  lang: "en-US",
  cleanUrls: true,
  lastUpdated: true,

  // Deployed under https://docs.datannur.com/builder/
  base: "/builder/",

  head: [
    [
      "link",
      {
        rel: "icon",
        type: "image/png",
        sizes: "32x32",
        href: "/builder/favicon-32.png",
      },
    ],
  ],

  themeConfig: {
    logo: "/logo.png",

    nav: [
      { text: "Website", link: "https://datannur.com" },
      { text: "Demo", link: "https://dev.datannur.com/" },
      { text: "App docs", link: "https://docs.datannur.com/app/" },
    ],

    sidebar: [
      { text: "Getting Started", link: "/" },
      { text: "Scanning files", link: "/scanning-files" },
      { text: "Scanning databases", link: "/scanning-databases" },
      { text: "Remote storage", link: "/remote-storage" },
      { text: "Metadata & configuration", link: "/metadata" },
      { text: "Output & exports", link: "/output" },
      { text: "Python API", link: "/python-api" },
    ],

    socialLinks: [
      { icon: "github", link: "https://github.com/datannur/datannurpy" },
    ],

    search: { provider: "local" },

    editLink: {
      pattern: "https://github.com/datannur/datannurpy/edit/main/docs/:path",
      text: "Edit this page on GitHub",
    },
  },
});
