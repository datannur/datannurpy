import { defineConfig, type DefaultTheme } from "vitepress";

const sidebar = {
  en: [
    { text: "Getting Started", link: "/" },
    { text: "Scan depth", link: "/scan-depth" },
    { text: "Scanning files", link: "/scanning-files" },
    { text: "Scanning databases", link: "/scanning-databases" },
    { text: "Metadata & configuration", link: "/metadata" },
    { text: "Output & exports", link: "/output" },
    { text: "Time series grouping", link: "/time-series" },
    { text: "Python API", link: "/python-api" },
  ],
  de: [
    { text: "Erste Schritte", link: "/de/" },
    { text: "Scan-Tiefe", link: "/de/scan-depth" },
    { text: "Dateien scannen", link: "/de/scanning-files" },
    { text: "Datenbanken scannen", link: "/de/scanning-databases" },
    { text: "Metadaten & Konfiguration", link: "/de/metadata" },
    { text: "Ausgabe & Exporte", link: "/de/output" },
    { text: "Zeitreihen-Gruppierung", link: "/de/time-series" },
    { text: "Python-API", link: "/de/python-api" },
  ],
  fr: [
    { text: "Démarrage", link: "/fr/" },
    { text: "Profondeur de scan", link: "/fr/scan-depth" },
    { text: "Scanner des fichiers", link: "/fr/scanning-files" },
    { text: "Scanner des bases de données", link: "/fr/scanning-databases" },
    { text: "Métadonnées & configuration", link: "/fr/metadata" },
    { text: "Sortie & exports", link: "/fr/output" },
    { text: "Regroupement de séries temporelles", link: "/fr/time-series" },
    { text: "API Python", link: "/fr/python-api" },
  ],
  it: [
    { text: "Per iniziare", link: "/it/" },
    { text: "Profondità di scansione", link: "/it/scan-depth" },
    { text: "Scansione dei file", link: "/it/scanning-files" },
    { text: "Scansione dei database", link: "/it/scanning-databases" },
    { text: "Metadati & configurazione", link: "/it/metadata" },
    { text: "Output & esportazioni", link: "/it/output" },
    { text: "Raggruppamento di serie temporali", link: "/it/time-series" },
    { text: "API Python", link: "/it/python-api" },
  ],
} satisfies Record<string, DefaultTheme.Sidebar>;

const nav = {
  en: [
    { text: "Website", link: "https://datannur.com" },
    { text: "Demo", link: "https://dev.datannur.com/" },
    { text: "App docs", link: "https://docs.datannur.com/app/" },
  ],
  de: [
    { text: "Website", link: "https://datannur.com" },
    { text: "Demo", link: "https://dev.datannur.com/" },
    { text: "App-Doku", link: "https://docs.datannur.com/app/" },
  ],
  fr: [
    { text: "Site web", link: "https://datannur.com" },
    { text: "Démo", link: "https://dev.datannur.com/" },
    { text: "Doc de l'app", link: "https://docs.datannur.com/app/" },
  ],
  it: [
    { text: "Sito web", link: "https://datannur.com" },
    { text: "Demo", link: "https://dev.datannur.com/" },
    { text: "Doc dell'app", link: "https://docs.datannur.com/app/" },
  ],
} satisfies Record<string, DefaultTheme.NavItem[]>;

// https://vitepress.dev/reference/site-config
export default defineConfig({
  title: "datannur builder docs",
  description: "Build and export datannur catalogs from files and databases",
  cleanUrls: true,
  lastUpdated: true,

  // Deployed under https://docs.datannur.com/builder/
  base: "/builder/",

  head: [
    [
      "link",
      {
        rel: "icon",
        type: "image/x-icon",
        href: "/builder/icon.ico",
      },
    ],
  ],

  locales: {
    root: {
      label: "English",
      lang: "en-US",
      themeConfig: {
        nav: nav.en,
        sidebar: sidebar.en,
        editLink: {
          pattern:
            "https://github.com/datannur/datannurpy/edit/main/docs/:path",
          text: "Edit this page on GitHub",
        },
      },
    },
    de: {
      label: "Deutsch",
      lang: "de-DE",
      description:
        "datannur-Kataloge aus Dateien und Datenbanken erstellen und exportieren",
      themeConfig: {
        nav: nav.de,
        sidebar: sidebar.de,
        editLink: {
          pattern:
            "https://github.com/datannur/datannurpy/edit/main/docs/:path",
          text: "Diese Seite auf GitHub bearbeiten",
        },
        outlineTitle: "Auf dieser Seite",
        lastUpdatedText: "Zuletzt aktualisiert",
        docFooter: { prev: "Vorherige Seite", next: "Nächste Seite" },
        darkModeSwitchLabel: "Erscheinungsbild",
        lightModeSwitchTitle: "Zum hellen Modus wechseln",
        darkModeSwitchTitle: "Zum dunklen Modus wechseln",
        sidebarMenuLabel: "Menü",
        returnToTopLabel: "Nach oben",
        langMenuLabel: "Sprache ändern",
        skipToContentLabel: "Zum Inhalt springen",
        notFound: {
          title: "SEITE NICHT GEFUNDEN",
          quote:
            "Aber wenn du die Richtung nicht änderst und weitergehst, könntest du dort landen, wohin du unterwegs bist.",
          linkLabel: "Zur Startseite",
          linkText: "Zur Startseite",
        },
      },
    },
    fr: {
      label: "Français",
      lang: "fr-FR",
      description:
        "Construire et exporter des catalogues datannur à partir de fichiers et de bases de données",
      themeConfig: {
        nav: nav.fr,
        sidebar: sidebar.fr,
        editLink: {
          pattern:
            "https://github.com/datannur/datannurpy/edit/main/docs/:path",
          text: "Modifier cette page sur GitHub",
        },
        outlineTitle: "Sur cette page",
        lastUpdatedText: "Dernière mise à jour",
        docFooter: { prev: "Page précédente", next: "Page suivante" },
        darkModeSwitchLabel: "Apparence",
        lightModeSwitchTitle: "Passer au mode clair",
        darkModeSwitchTitle: "Passer au mode sombre",
        sidebarMenuLabel: "Menu",
        returnToTopLabel: "Retour en haut",
        langMenuLabel: "Changer de langue",
        skipToContentLabel: "Aller au contenu",
        notFound: {
          title: "PAGE INTROUVABLE",
          quote:
            "Mais si tu ne changes pas de direction et que tu continues, tu risques d'arriver là où tu te diriges.",
          linkLabel: "Retour à l'accueil",
          linkText: "Retour à l'accueil",
        },
      },
    },
    it: {
      label: "Italiano",
      lang: "it-IT",
      description:
        "Costruisci ed esporta cataloghi datannur da file e database",
      themeConfig: {
        nav: nav.it,
        sidebar: sidebar.it,
        editLink: {
          pattern:
            "https://github.com/datannur/datannurpy/edit/main/docs/:path",
          text: "Modifica questa pagina su GitHub",
        },
        outlineTitle: "In questa pagina",
        lastUpdatedText: "Ultimo aggiornamento",
        docFooter: { prev: "Pagina precedente", next: "Pagina successiva" },
        darkModeSwitchLabel: "Aspetto",
        lightModeSwitchTitle: "Passa alla modalità chiara",
        darkModeSwitchTitle: "Passa alla modalità scura",
        sidebarMenuLabel: "Menu",
        returnToTopLabel: "Torna in alto",
        langMenuLabel: "Cambia lingua",
        skipToContentLabel: "Vai al contenuto",
        notFound: {
          title: "PAGINA NON TROVATA",
          quote:
            "Ma se non cambi direzione e continui a cercare, potresti finire dove sei diretto.",
          linkLabel: "Torna alla home",
          linkText: "Torna alla home",
        },
      },
    },
  },

  themeConfig: {
    logo: "/icon.svg",

    outline: "deep",

    socialLinks: [
      { icon: "github", link: "https://github.com/datannur/datannurpy" },
    ],

    search: {
      provider: "local",
      options: {
        locales: {
          de: {
            translations: {
              button: {
                buttonText: "Suchen",
                buttonAriaLabel: "Suchen",
              },
              modal: {
                displayDetails: "Detailansicht anzeigen",
                resetButtonTitle: "Suche zurücksetzen",
                backButtonTitle: "Suche schließen",
                noResultsText: "Keine Ergebnisse für",
                footer: {
                  selectText: "auswählen",
                  selectKeyAriaLabel: "Eingabetaste",
                  navigateText: "navigieren",
                  navigateUpKeyAriaLabel: "Pfeil nach oben",
                  navigateDownKeyAriaLabel: "Pfeil nach unten",
                  closeText: "schließen",
                  closeKeyAriaLabel: "Escape",
                },
              },
            },
          },
          fr: {
            translations: {
              button: {
                buttonText: "Rechercher",
                buttonAriaLabel: "Rechercher",
              },
              modal: {
                displayDetails: "Afficher la vue détaillée",
                resetButtonTitle: "Réinitialiser la recherche",
                backButtonTitle: "Fermer la recherche",
                noResultsText: "Aucun résultat pour",
                footer: {
                  selectText: "sélectionner",
                  selectKeyAriaLabel: "Entrée",
                  navigateText: "naviguer",
                  navigateUpKeyAriaLabel: "Flèche haut",
                  navigateDownKeyAriaLabel: "Flèche bas",
                  closeText: "fermer",
                  closeKeyAriaLabel: "Échap",
                },
              },
            },
          },
          it: {
            translations: {
              button: {
                buttonText: "Cerca",
                buttonAriaLabel: "Cerca",
              },
              modal: {
                displayDetails: "Mostra vista dettagliata",
                resetButtonTitle: "Reimposta ricerca",
                backButtonTitle: "Chiudi ricerca",
                noResultsText: "Nessun risultato per",
                footer: {
                  selectText: "seleziona",
                  selectKeyAriaLabel: "Invio",
                  navigateText: "naviga",
                  navigateUpKeyAriaLabel: "Freccia su",
                  navigateDownKeyAriaLabel: "Freccia giù",
                  closeText: "chiudi",
                  closeKeyAriaLabel: "Esc",
                },
              },
            },
          },
        },
      },
    },
  },
});
