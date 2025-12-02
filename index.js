require("dotenv").config();
const express = require("express");
const cors = require("cors");
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const PDFDocument = require("pdfkit");
const Epub = require("epub");
const app = express();

const PORT = process.env.PORT || 4000;
const API_KEY = process.env.API_KEY || "my-api-key-123";

const cache = new Map();
const CACHE_TTL = 10 * 60 * 1000;

const apiStats = {
  startTime: Date.now(),
  totalRequests: 0,
  endpoints: {},
  errors: 0,
  sourceHits: { gutendex: 0, openlibrary: 0, internetarchive: 0, googlebooks: 0 },
  lastRequests: []
};

const rateLimitMap = new Map();
const RATE_LIMIT = 100;
const RATE_WINDOW = 60 * 1000;

const LANGUAGES = {
  sn: { name: "Shona", region: "africa", native: "chiShona" },
  sw: { name: "Swahili", region: "africa", native: "Kiswahili" },
  zu: { name: "Zulu", region: "africa", native: "isiZulu" },
  xh: { name: "Xhosa", region: "africa", native: "isiXhosa" },
  af: { name: "Afrikaans", region: "africa", native: "Afrikaans" },
  am: { name: "Amharic", region: "africa", native: "አማርኛ" },
  ha: { name: "Hausa", region: "africa", native: "Hausa" },
  yo: { name: "Yoruba", region: "africa", native: "Yorùbá" },
  ig: { name: "Igbo", region: "africa", native: "Igbo" },
  ar: { name: "Arabic", region: "middleeast", native: "العربية" },
  en: { name: "English", region: "global", native: "English" },
  fr: { name: "French", region: "europe", native: "Français" },
  es: { name: "Spanish", region: "europe", native: "Español" },
  pt: { name: "Portuguese", region: "europe", native: "Português" },
  de: { name: "German", region: "europe", native: "Deutsch" },
  it: { name: "Italian", region: "europe", native: "Italiano" },
  ru: { name: "Russian", region: "europe", native: "Русский" },
  zh: { name: "Chinese", region: "asia", native: "中文" },
  ja: { name: "Japanese", region: "asia", native: "日本語" },
  ko: { name: "Korean", region: "asia", native: "한국어" },
  hi: { name: "Hindi", region: "asia", native: "हिन्दी" },
  bn: { name: "Bengali", region: "asia", native: "বাংলা" },
  ta: { name: "Tamil", region: "asia", native: "தமிழ்" },
  te: { name: "Telugu", region: "asia", native: "తెలుగు" },
  th: { name: "Thai", region: "asia", native: "ไทย" },
  vi: { name: "Vietnamese", region: "asia", native: "Tiếng Việt" },
  id: { name: "Indonesian", region: "asia", native: "Bahasa Indonesia" },
  ms: { name: "Malay", region: "asia", native: "Bahasa Melayu" },
  tl: { name: "Tagalog", region: "asia", native: "Tagalog" },
  nl: { name: "Dutch", region: "europe", native: "Nederlands" },
  pl: { name: "Polish", region: "europe", native: "Polski" },
  tr: { name: "Turkish", region: "europe", native: "Türkçe" },
  el: { name: "Greek", region: "europe", native: "Ελληνικά" },
  he: { name: "Hebrew", region: "middleeast", native: "עברית" },
  fa: { name: "Persian", region: "middleeast", native: "فارسی" },
  ur: { name: "Urdu", region: "asia", native: "اردو" }
};

const REGIONS = {
  africa: {
    name: "Africa",
    languages: ["sn", "sw", "zu", "xh", "af", "am", "ha", "yo", "ig", "ar"],
    keywords: ["africa", "african", "zimbabwe", "kenya", "nigeria", "south africa", "ethiopia", "ghana", "tanzania"]
  },
  asia: {
    name: "Asia",
    languages: ["zh", "ja", "ko", "hi", "bn", "ta", "te", "th", "vi", "id", "ms", "tl", "ur"],
    keywords: ["asia", "asian", "china", "japan", "india", "korea", "vietnam", "thailand", "indonesia"]
  },
  europe: {
    name: "Europe",
    languages: ["en", "fr", "es", "pt", "de", "it", "ru", "nl", "pl", "tr", "el"],
    keywords: ["europe", "european", "britain", "france", "germany", "spain", "italy", "russia"]
  },
  americas: {
    name: "Americas",
    languages: ["en", "es", "pt", "fr"],
    keywords: ["america", "american", "usa", "canada", "brazil", "mexico", "latin america"]
  },
  middleeast: {
    name: "Middle East",
    languages: ["ar", "he", "fa", "tr"],
    keywords: ["middle east", "arab", "persian", "israel", "iran", "turkey"]
  }
};

const COLLECTIONS = {
  classics: { name: "Literary Classics", topic: "fiction", description: "Timeless works of literature" },
  scifi: { name: "Science Fiction", topic: "science fiction", description: "Sci-fi adventures and futuristic tales" },
  mystery: { name: "Mystery & Detective", topic: "detective", description: "Whodunits and crime fiction" },
  romance: { name: "Romance", topic: "love stories", description: "Love stories and romantic fiction" },
  adventure: { name: "Adventure", topic: "adventure", description: "Action-packed adventure stories" },
  horror: { name: "Horror & Gothic", topic: "horror", description: "Scary stories and gothic fiction" },
  philosophy: { name: "Philosophy", topic: "philosophy", description: "Philosophical works and essays" },
  poetry: { name: "Poetry", topic: "poetry", description: "Poems and verse collections" },
  history: { name: "History", topic: "history", description: "Historical works and biographies" },
  children: { name: "Children's Books", topic: "children", description: "Books for young readers" },
  african: { name: "African Literature", topic: "africa", description: "Literature from the African continent" },
  folktales: { name: "Folk Tales & Mythology", topic: "folklore", description: "Traditional stories and myths" },
  religious: { name: "Religious Texts", topic: "religion", description: "Sacred and spiritual texts" },
  education: { name: "Educational", topic: "education", description: "Learning and educational materials" },
  biography: { name: "Biographies", topic: "biography", description: "Life stories of notable people" }
};

const SUBJECTS = [
  "Fiction", "Science Fiction", "Fantasy", "Mystery", "Romance",
  "Horror", "Adventure", "Poetry", "Drama", "History",
  "Philosophy", "Science", "Biography", "Children's Literature",
  "Short Stories", "Humor", "Religion", "Politics", "Travel", "Essays",
  "African Literature", "Folklore", "Mythology", "Education", "Art",
  "Music", "Cooking", "Health", "Psychology", "Economics", "Law"
];

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(__dirname));

app.use((req, res, next) => {
  res.setHeader('Cache-Control', 'no-cache');
  next();
});

function getCached(key) {
  const item = cache.get(key);
  if (item && Date.now() - item.timestamp < CACHE_TTL) {
    return item.data;
  }
  cache.delete(key);
  return null;
}

function setCache(key, data) {
  cache.set(key, { data, timestamp: Date.now() });
}

function trackRequest(req) {
  apiStats.totalRequests++;
  const endpoint = req.path;
  apiStats.endpoints[endpoint] = (apiStats.endpoints[endpoint] || 0) + 1;
  apiStats.lastRequests.unshift({
    endpoint,
    method: req.method,
    timestamp: new Date().toISOString()
  });
  if (apiStats.lastRequests.length > 50) {
    apiStats.lastRequests.pop();
  }
}

function checkRateLimit(apiKey) {
  const now = Date.now();
  const windowStart = now - RATE_WINDOW;
  
  if (!rateLimitMap.has(apiKey)) {
    rateLimitMap.set(apiKey, []);
  }
  
  const requests = rateLimitMap.get(apiKey).filter(time => time > windowStart);
  rateLimitMap.set(apiKey, requests);
  
  if (requests.length >= RATE_LIMIT) {
    return false;
  }
  
  requests.push(now);
  return true;
}

function requireApiKey(req, res, next) {
  const key = req.header("x-api-key");
  if (!key || key !== API_KEY) {
    return res.status(401).json({ 
      error: "Invalid API key",
      message: "Please provide a valid API key in the x-api-key header"
    });
  }
  
  if (!checkRateLimit(key)) {
    return res.status(429).json({
      error: "Rate limit exceeded",
      message: `Maximum ${RATE_LIMIT} requests per minute allowed`,
      retry_after: 60
    });
  }
  
  trackRequest(req);
  next();
}

async function fetchWithFallback(urls, options = {}) {
  for (const url of urls) {
    try {
      const response = await axios({
        url,
        method: 'GET',
        timeout: options.timeout || 15000,
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        },
        ...options
      });
      return { success: true, data: response.data, url };
    } catch (error) {
      console.log(`Fallback: ${url} failed - ${error.message}`);
      continue;
    }
  }
  return { success: false, error: "All sources failed" };
}

async function searchGutendex(query, options = {}) {
  try {
    const params = new URLSearchParams();
    if (query) params.append('search', query);
    if (options.language) params.append('languages', options.language);
    if (options.topic) params.append('topic', options.topic);
    if (options.author) params.append('search', options.author);
    if (options.page) params.append('page', options.page);
    params.append('sort', options.sort || 'popular');
    
    const url = `https://gutendex.com/books/?${params.toString()}`;
    console.log(`Gutendex search: ${url}`);
    
    const response = await axios.get(url, { timeout: 15000 });
    apiStats.sourceHits.gutendex++;
    
    return {
      success: true,
      source: 'gutendex',
      books: response.data.results.map(formatGutendexBook),
      total: response.data.count,
      next: response.data.next,
      previous: response.data.previous
    };
  } catch (error) {
    console.error('Gutendex error:', error.message);
    return { success: false, source: 'gutendex', error: error.message };
  }
}

async function searchOpenLibrary(query, options = {}) {
  try {
    const params = new URLSearchParams();
    params.append('q', query);
    if (options.language) params.append('language', options.language);
    if (options.author) params.append('author', options.author);
    params.append('limit', options.limit || 20);
    params.append('offset', ((options.page || 1) - 1) * (options.limit || 20));
    
    const url = `https://openlibrary.org/search.json?${params.toString()}`;
    console.log(`OpenLibrary search: ${url}`);
    
    const response = await axios.get(url, { timeout: 15000 });
    apiStats.sourceHits.openlibrary++;
    
    return {
      success: true,
      source: 'openlibrary',
      books: response.data.docs.map(formatOpenLibraryBook),
      total: response.data.numFound
    };
  } catch (error) {
    console.error('OpenLibrary error:', error.message);
    return { success: false, source: 'openlibrary', error: error.message };
  }
}

async function searchInternetArchive(query, options = {}) {
  try {
    const params = new URLSearchParams();
    
    let searchQuery = `${query} AND mediatype:texts`;
    if (options.language) {
      searchQuery += ` AND language:${options.language}`;
    }
    
    params.append('q', searchQuery);
    params.append('fl[]', 'identifier,title,creator,description,language,year,downloads,subject');
    params.append('rows', options.limit || 20);
    params.append('page', options.page || 1);
    params.append('output', 'json');
    
    const url = `https://archive.org/advancedsearch.php?${params.toString()}`;
    console.log(`Internet Archive search: ${url}`);
    
    const response = await axios.get(url, { timeout: 20000 });
    apiStats.sourceHits.internetarchive++;
    
    const docs = response.data.response?.docs || [];
    
    return {
      success: true,
      source: 'internetarchive',
      books: docs.map(formatInternetArchiveBook),
      total: response.data.response?.numFound || 0
    };
  } catch (error) {
    console.error('Internet Archive error:', error.message);
    return { success: false, source: 'internetarchive', error: error.message };
  }
}

async function searchGoogleBooks(query, options = {}) {
  try {
    const params = new URLSearchParams();
    
    let q = query;
    if (options.author) q += `+inauthor:${options.author}`;
    if (options.language) params.append('langRestrict', options.language);
    
    params.append('q', q);
    params.append('maxResults', Math.min(options.limit || 20, 40));
    params.append('startIndex', ((options.page || 1) - 1) * (options.limit || 20));
    params.append('printType', 'books');
    
    const url = `https://www.googleapis.com/books/v1/volumes?${params.toString()}`;
    console.log(`Google Books search: ${url}`);
    
    const response = await axios.get(url, { timeout: 15000 });
    apiStats.sourceHits.googlebooks++;
    
    const items = response.data.items || [];
    
    return {
      success: true,
      source: 'googlebooks',
      books: items.map(formatGoogleBook),
      total: response.data.totalItems || 0
    };
  } catch (error) {
    console.error('Google Books error:', error.message);
    return { success: false, source: 'googlebooks', error: error.message };
  }
}

function formatGutendexBook(b) {
  const id = b.id;
  return {
    id: `gutendex:${id}`,
    source: "gutendex",
    title: b.title,
    authors: b.authors.map(a => ({
      name: a.name,
      birth_year: a.birth_year,
      death_year: a.death_year,
      profile_url: `/v1/authors/${encodeURIComponent(a.name)}/profile`
    })),
    description: b.subjects?.join(', ') || null,
    subjects: b.subjects || [],
    bookshelves: b.bookshelves || [],
    languages: b.languages || [],
    download_count: b.download_count,
    covers: {
      small: b.formats["image/jpeg"] || null,
      medium: b.formats["image/jpeg"]?.replace('-small', '-medium') || b.formats["image/jpeg"] || null,
      large: b.formats["image/jpeg"]?.replace('-small', '') || b.formats["image/jpeg"] || null
    },
    formats: {
      epub: b.formats["application/epub+zip"] || null,
      pdf: "available",
      txt: b.formats["text/plain; charset=us-ascii"] || b.formats["text/plain; charset=utf-8"] || b.formats["text/plain"] || null,
      html: b.formats["text/html"] || null,
      mobi: b.formats["application/x-mobipocket-ebook"] || null
    },
    download_url: `/v1/download/gutendex/${id}`,
    details_url: `/v1/books/gutendex/${id}`,
    external_links: {
      gutenberg: `https://www.gutenberg.org/ebooks/${id}`,
      wikipedia: b.authors[0] ? `https://en.wikipedia.org/wiki/${encodeURIComponent(b.authors[0].name.replace(/, /g, '_'))}` : null
    },
    copyright: b.copyright
  };
}

function formatOpenLibraryBook(b) {
  const key = b.key?.replace('/works/', '') || b.key;
  const coverId = b.cover_i;
  
  return {
    id: `openlibrary:${key}`,
    source: "openlibrary",
    title: b.title,
    authors: (b.author_name || []).map((name, i) => ({
      name,
      key: b.author_key?.[i] || null,
      profile_url: b.author_key?.[i] ? `/v1/authors/ol:${b.author_key[i]}/profile` : null
    })),
    description: b.first_sentence?.join(' ') || null,
    first_publish_year: b.first_publish_year,
    languages: b.language || [],
    subjects: b.subject?.slice(0, 15) || [],
    edition_count: b.edition_count,
    ebook_count: b.ebook_count_i || 0,
    has_fulltext: b.has_fulltext || false,
    covers: {
      small: coverId ? `https://covers.openlibrary.org/b/id/${coverId}-S.jpg` : null,
      medium: coverId ? `https://covers.openlibrary.org/b/id/${coverId}-M.jpg` : null,
      large: coverId ? `https://covers.openlibrary.org/b/id/${coverId}-L.jpg` : null
    },
    formats: {
      epub: b.has_fulltext ? `https://openlibrary.org${b.key}.epub` : null,
      pdf: b.has_fulltext ? `https://openlibrary.org${b.key}.pdf` : null
    },
    details_url: `/v1/books/openlibrary/${key}`,
    external_links: {
      openlibrary: `https://openlibrary.org${b.key}`,
      goodreads: b.id_goodreads?.[0] ? `https://www.goodreads.com/book/show/${b.id_goodreads[0]}` : null,
      amazon: b.id_amazon?.[0] ? `https://www.amazon.com/dp/${b.id_amazon[0]}` : null
    }
  };
}

function formatInternetArchiveBook(b) {
  const id = b.identifier;
  
  return {
    id: `archive:${id}`,
    source: "internetarchive",
    title: b.title,
    authors: Array.isArray(b.creator) ? b.creator.map(name => ({ name })) : (b.creator ? [{ name: b.creator }] : []),
    description: Array.isArray(b.description) ? b.description[0] : b.description,
    year: b.year,
    languages: Array.isArray(b.language) ? b.language : (b.language ? [b.language] : []),
    subjects: Array.isArray(b.subject) ? b.subject.slice(0, 15) : (b.subject ? [b.subject] : []),
    download_count: b.downloads || 0,
    covers: {
      small: `https://archive.org/services/img/${id}`,
      medium: `https://archive.org/services/img/${id}`,
      large: `https://archive.org/services/img/${id}`
    },
    formats: {
      epub: `https://archive.org/download/${id}/${id}.epub`,
      pdf: `https://archive.org/download/${id}/${id}.pdf`,
      txt: `https://archive.org/download/${id}/${id}_djvu.txt`,
      mobi: `https://archive.org/download/${id}/${id}.mobi`
    },
    download_url: `/v1/download/archive/${id}`,
    details_url: `/v1/books/archive/${id}`,
    external_links: {
      archive: `https://archive.org/details/${id}`,
      download_page: `https://archive.org/download/${id}`
    }
  };
}

function formatGoogleBook(b) {
  const info = b.volumeInfo || {};
  const id = b.id;
  
  return {
    id: `google:${id}`,
    source: "googlebooks",
    title: info.title,
    subtitle: info.subtitle,
    authors: (info.authors || []).map(name => ({ name })),
    description: info.description,
    publisher: info.publisher,
    published_date: info.publishedDate,
    page_count: info.pageCount,
    categories: info.categories || [],
    languages: [info.language].filter(Boolean),
    average_rating: info.averageRating,
    ratings_count: info.ratingsCount,
    covers: {
      small: info.imageLinks?.smallThumbnail || null,
      medium: info.imageLinks?.thumbnail || null,
      large: info.imageLinks?.large || info.imageLinks?.thumbnail?.replace('zoom=1', 'zoom=2') || null
    },
    formats: {
      epub: b.accessInfo?.epub?.isAvailable ? b.accessInfo.epub.downloadLink : null,
      pdf: b.accessInfo?.pdf?.isAvailable ? b.accessInfo.pdf.downloadLink : null,
      webReader: info.previewLink
    },
    preview_available: b.accessInfo?.viewability !== 'NO_PAGES',
    details_url: `/v1/books/google/${id}`,
    external_links: {
      googlebooks: info.infoLink,
      preview: info.previewLink,
      buy: b.saleInfo?.buyLink
    },
    isbn: {
      isbn10: info.industryIdentifiers?.find(i => i.type === 'ISBN_10')?.identifier,
      isbn13: info.industryIdentifiers?.find(i => i.type === 'ISBN_13')?.identifier
    }
  };
}

async function downloadFile(url, filePath) {
  const response = await axios({
    method: 'GET',
    url: url,
    responseType: 'stream',
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    },
    timeout: 30000
  });

  return new Promise((resolve, reject) => {
    const writer = fs.createWriteStream(filePath);
    response.data.pipe(writer);
    writer.on('finish', resolve);
    writer.on('error', reject);
  });
}

function extractTextFromEpub(epubPath) {
  return new Promise((resolve, reject) => {
    const epub = new Epub(epubPath);
    let fullText = '';

    epub.on('error', (err) => {
      console.error('EPUB parsing error:', err);
      reject(err);
    });

    epub.on('end', function() {
      if (!epub.flow || epub.flow.length === 0) {
        resolve('No content found in EPUB');
        return;
      }

      const chapterIds = epub.flow.map(chapter => chapter.id);
      let processedChapters = 0;

      chapterIds.forEach(chapterId => {
        epub.getChapter(chapterId, (err, text) => {
          if (err) {
            console.error(`Error reading chapter ${chapterId}:`, err);
          } else {
            const cleanText = text.toString()
              .replace(/<[^>]*>/g, ' ')
              .replace(/\s+/g, ' ')
              .trim();
            fullText += cleanText + '\n\n';
          }

          processedChapters++;
          if (processedChapters === chapterIds.length) {
            resolve(fullText);
          }
        });
      });
    });

    epub.parse();
  });
}

app.get("/", (req, res) => {
  res.json({ 
    name: "Global Book API",
    version: "3.0",
    description: "Multi-source international book search with African literature support",
    documentation: "/v1/docs",
    health: "/v1/health",
    sources: ["Gutendex (Project Gutenberg)", "OpenLibrary", "Internet Archive", "Google Books"],
    features: [
      "Search books in 30+ languages including Shona, Swahili, and other African languages",
      "Regional book discovery (Africa, Asia, Europe, Americas, Middle East)",
      "Multiple download formats (EPUB, PDF, TXT, MOBI)",
      "Book covers in multiple sizes",
      "Author profiles with external links",
      "Curated collections by genre and region",
      "Fallback between multiple sources"
    ],
    endpoints: {
      search: {
        unified: "GET /v1/search?q=query",
        advanced: "GET /v1/search/advanced",
        by_source: "GET /v1/sources/:source/search?q=query"
      },
      languages: {
        list: "GET /v1/languages",
        books: "GET /v1/languages/:code/books"
      },
      regions: {
        list: "GET /v1/regions",
        books: "GET /v1/regions/:region/books"
      },
      discovery: {
        trending: "GET /v1/trending",
        random: "GET /v1/random",
        collections: "GET /v1/collections",
        african: "GET /v1/highlights/africa",
        shona: "GET /v1/highlights/shona"
      },
      books: {
        details: "GET /v1/books/:source/:id",
        covers: "GET /v1/books/:source/:id/covers",
        formats: "GET /v1/books/:source/:id/formats",
        similar: "GET /v1/books/:source/:id/similar",
        download: "GET /v1/download/:source/:id"
      },
      authors: {
        search: "GET /v1/authors?q=name",
        books: "GET /v1/authors/:name/books",
        profile: "GET /v1/authors/:name/profile"
      },
      utilities: {
        stats: "GET /v1/stats",
        health: "GET /v1/health",
        docs: "GET /v1/docs"
      }
    },
    authentication: "Required: x-api-key header",
    rate_limit: `${RATE_LIMIT} requests per minute`
  });
});

app.get("/v1/health", (req, res) => {
  const uptime = Math.floor((Date.now() - apiStats.startTime) / 1000);
  res.json({
    status: "healthy",
    version: "3.0",
    uptime_seconds: uptime,
    uptime_formatted: `${Math.floor(uptime / 3600)}h ${Math.floor((uptime % 3600) / 60)}m ${uptime % 60}s`,
    sources: {
      gutendex: { status: "active", hits: apiStats.sourceHits.gutendex },
      openlibrary: { status: "active", hits: apiStats.sourceHits.openlibrary },
      internetarchive: { status: "active", hits: apiStats.sourceHits.internetarchive },
      googlebooks: { status: "active", hits: apiStats.sourceHits.googlebooks }
    },
    cache_size: cache.size,
    memory: {
      used_mb: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
      total_mb: Math.round(process.memoryUsage().heapTotal / 1024 / 1024)
    },
    timestamp: new Date().toISOString()
  });
});

app.get("/v1/stats", requireApiKey, (req, res) => {
  const uptime = Math.floor((Date.now() - apiStats.startTime) / 1000);
  
  const sortedEndpoints = Object.entries(apiStats.endpoints)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 15);
  
  res.json({
    total_requests: apiStats.totalRequests,
    errors: apiStats.errors,
    uptime_seconds: uptime,
    requests_per_minute: apiStats.totalRequests / (uptime / 60) || 0,
    source_hits: apiStats.sourceHits,
    top_endpoints: Object.fromEntries(sortedEndpoints),
    cache_entries: cache.size,
    supported_languages: Object.keys(LANGUAGES).length,
    supported_regions: Object.keys(REGIONS).length,
    recent_requests: apiStats.lastRequests.slice(0, 10)
  });
});

app.get("/v1/docs", (req, res) => {
  res.json({
    title: "Global Book API Documentation",
    version: "3.0",
    base_url: req.protocol + '://' + req.get('host'),
    sources: [
      { name: "Gutendex", description: "Project Gutenberg - 70,000+ free ebooks", url: "https://gutendex.com" },
      { name: "OpenLibrary", description: "Open Library - millions of books", url: "https://openlibrary.org" },
      { name: "Internet Archive", description: "Archive.org - digital library", url: "https://archive.org" },
      { name: "Google Books", description: "Google Books API - broad catalog", url: "https://books.google.com" }
    ],
    african_languages: Object.entries(LANGUAGES)
      .filter(([code, lang]) => lang.region === 'africa')
      .map(([code, lang]) => ({ code, name: lang.name, native: lang.native })),
    authentication: {
      type: "API Key",
      header: "x-api-key",
      description: "Include your API key in all requests (except / and /v1/health)"
    },
    rate_limiting: {
      limit: RATE_LIMIT,
      window: "1 minute"
    },
    endpoints: [
      {
        path: "/v1/search",
        method: "GET",
        description: "Unified search across all sources with fallback",
        parameters: {
          q: { type: "string", required: true, description: "Search query" },
          sources: { type: "string", description: "Comma-separated sources (gutendex,openlibrary,archive,google)" },
          language: { type: "string", description: "Language code (en, sn, sw, etc.)" },
          limit: { type: "integer", default: 20 }
        }
      },
      {
        path: "/v1/languages",
        method: "GET",
        description: "List all supported languages including African languages"
      },
      {
        path: "/v1/languages/:code/books",
        method: "GET",
        description: "Search books in a specific language",
        parameters: {
          code: { description: "Language code (sn=Shona, sw=Swahili, etc.)" },
          q: { description: "Optional search query" },
          limit: { default: 20 }
        }
      },
      {
        path: "/v1/regions",
        method: "GET",
        description: "List available regions (Africa, Asia, Europe, etc.)"
      },
      {
        path: "/v1/regions/:region/books",
        method: "GET",
        description: "Get books from a specific region",
        parameters: {
          region: { description: "Region name (africa, asia, europe, americas, middleeast)" }
        }
      },
      {
        path: "/v1/highlights/africa",
        method: "GET",
        description: "Curated African literature and books about Africa"
      },
      {
        path: "/v1/highlights/shona",
        method: "GET",
        description: "Books in Shona language or about Zimbabwe/Shona culture"
      },
      {
        path: "/v1/sources/:source/search",
        method: "GET",
        description: "Search a specific source directly",
        parameters: {
          source: { description: "Source name (gutendex, openlibrary, archive, google)" },
          q: { required: true }
        }
      },
      {
        path: "/v1/books/:source/:id",
        method: "GET",
        description: "Get book details from a specific source"
      },
      {
        path: "/v1/books/:source/:id/covers",
        method: "GET",
        description: "Get book cover images in multiple sizes"
      },
      {
        path: "/v1/books/:source/:id/formats",
        method: "GET",
        description: "Get available download formats for a book"
      },
      {
        path: "/v1/download/:source/:id",
        method: "GET",
        description: "Download a book file",
        parameters: {
          format: { description: "Format: epub, pdf, txt, mobi", default: "epub" }
        }
      },
      {
        path: "/v1/authors/:name/profile",
        method: "GET",
        description: "Get author profile with books and external links"
      }
    ]
  });
});

app.get("/v1/languages", requireApiKey, (req, res) => {
  const languages = Object.entries(LANGUAGES).map(([code, lang]) => ({
    code,
    name: lang.name,
    native_name: lang.native,
    region: lang.region,
    books_url: `/v1/languages/${code}/books`
  }));
  
  const byRegion = {};
  languages.forEach(lang => {
    if (!byRegion[lang.region]) byRegion[lang.region] = [];
    byRegion[lang.region].push(lang);
  });
  
  res.json({
    data: languages,
    by_region: byRegion,
    meta: {
      total: languages.length,
      african_languages: languages.filter(l => l.region === 'africa').length
    }
  });
});

app.get("/v1/languages/:code/books", requireApiKey, async (req, res) => {
  const { code } = req.params;
  const { q, limit = 20, page = 1 } = req.query;
  
  const language = LANGUAGES[code.toLowerCase()];
  if (!language) {
    return res.status(404).json({
      error: "Language not found",
      available: Object.keys(LANGUAGES),
      message: "Use a valid language code"
    });
  }
  
  const cacheKey = `lang:${code}:${q || 'all'}:${page}`;
  const cached = getCached(cacheKey);
  if (cached) {
    return res.json({ ...cached, meta: { ...cached.meta, cached: true } });
  }
  
  try {
    const results = await Promise.allSettled([
      searchGutendex(q || language.name, { language: code, page, limit }),
      searchInternetArchive(q || language.name, { language: code, page, limit }),
      searchOpenLibrary(q || language.name, { language: code, page, limit }),
      searchGoogleBooks(q || language.name, { language: code, page, limit })
    ]);
    
    let allBooks = [];
    const sourceResults = {};
    
    results.forEach((result, index) => {
      const sources = ['gutendex', 'internetarchive', 'openlibrary', 'googlebooks'];
      if (result.status === 'fulfilled' && result.value.success) {
        allBooks = allBooks.concat(result.value.books);
        sourceResults[sources[index]] = {
          success: true,
          count: result.value.books.length
        };
      } else {
        sourceResults[sources[index]] = {
          success: false,
          error: result.reason?.message || result.value?.error
        };
      }
    });
    
    const response = {
      language: {
        code,
        name: language.name,
        native: language.native,
        region: language.region
      },
      data: allBooks.slice(0, parseInt(limit)),
      meta: {
        total: allBooks.length,
        page: parseInt(page),
        sources: sourceResults,
        cached: false
      }
    };
    
    setCache(cacheKey, response);
    res.json(response);
  } catch (err) {
    console.error('Language books error:', err);
    apiStats.errors++;
    res.status(500).json({ error: "Failed to fetch books", details: err.message });
  }
});

app.get("/v1/regions", requireApiKey, (req, res) => {
  const regions = Object.entries(REGIONS).map(([id, region]) => ({
    id,
    name: region.name,
    languages: region.languages.map(code => ({
      code,
      name: LANGUAGES[code]?.name
    })),
    keywords: region.keywords,
    books_url: `/v1/regions/${id}/books`
  }));
  
  res.json({
    data: regions,
    meta: { total: regions.length }
  });
});

app.get("/v1/regions/:region/books", requireApiKey, async (req, res) => {
  const { region } = req.params;
  const { q, limit = 20, page = 1 } = req.query;
  
  const regionData = REGIONS[region.toLowerCase()];
  if (!regionData) {
    return res.status(404).json({
      error: "Region not found",
      available: Object.keys(REGIONS)
    });
  }
  
  const cacheKey = `region:${region}:${q || 'all'}:${page}`;
  const cached = getCached(cacheKey);
  if (cached) {
    return res.json({ ...cached, meta: { ...cached.meta, cached: true } });
  }
  
  try {
    const searchQuery = q || regionData.keywords[0];
    
    const results = await Promise.allSettled([
      searchGutendex(searchQuery, { page, limit }),
      searchInternetArchive(searchQuery, { page, limit }),
      searchOpenLibrary(searchQuery, { page, limit }),
      searchGoogleBooks(searchQuery, { page, limit })
    ]);
    
    let allBooks = [];
    results.forEach(result => {
      if (result.status === 'fulfilled' && result.value.success) {
        allBooks = allBooks.concat(result.value.books);
      }
    });
    
    const response = {
      region: {
        id: region,
        name: regionData.name,
        languages: regionData.languages
      },
      data: allBooks.slice(0, parseInt(limit)),
      meta: {
        total: allBooks.length,
        page: parseInt(page),
        cached: false
      }
    };
    
    setCache(cacheKey, response);
    res.json(response);
  } catch (err) {
    console.error('Region books error:', err);
    apiStats.errors++;
    res.status(500).json({ error: "Failed to fetch books", details: err.message });
  }
});

app.get("/v1/highlights/africa", requireApiKey, async (req, res) => {
  const { limit = 30, page = 1 } = req.query;
  
  const cacheKey = `highlights:africa:${page}`;
  const cached = getCached(cacheKey);
  if (cached) {
    return res.json({ ...cached, meta: { ...cached.meta, cached: true } });
  }
  
  try {
    const searches = [
      searchGutendex("africa african", { page, limit: 10 }),
      searchInternetArchive("africa african literature", { page, limit: 10 }),
      searchOpenLibrary("african literature", { page, limit: 10 }),
      searchGoogleBooks("african literature", { page, limit: 10 })
    ];
    
    const results = await Promise.allSettled(searches);
    
    let allBooks = [];
    results.forEach(result => {
      if (result.status === 'fulfilled' && result.value.success) {
        allBooks = allBooks.concat(result.value.books);
      }
    });
    
    const response = {
      collection: {
        name: "African Literature",
        description: "Books from Africa and about African culture, history, and literature"
      },
      data: allBooks.slice(0, parseInt(limit)),
      meta: {
        total: allBooks.length,
        page: parseInt(page),
        cached: false
      }
    };
    
    setCache(cacheKey, response);
    res.json(response);
  } catch (err) {
    console.error('Africa highlights error:', err);
    apiStats.errors++;
    res.status(500).json({ error: "Failed to fetch African books", details: err.message });
  }
});

app.get("/v1/highlights/shona", requireApiKey, async (req, res) => {
  const { limit = 20, page = 1 } = req.query;
  
  const cacheKey = `highlights:shona:${page}`;
  const cached = getCached(cacheKey);
  if (cached) {
    return res.json({ ...cached, meta: { ...cached.meta, cached: true } });
  }
  
  try {
    const searches = [
      searchInternetArchive("shona language", { language: "sn", page, limit: 10 }),
      searchInternetArchive("zimbabwe", { page, limit: 10 }),
      searchGoogleBooks("shona zimbabwe", { page, limit: 10 }),
      searchOpenLibrary("shona zimbabwe", { page, limit: 10 })
    ];
    
    const results = await Promise.allSettled(searches);
    
    let allBooks = [];
    results.forEach(result => {
      if (result.status === 'fulfilled' && result.value.success) {
        allBooks = allBooks.concat(result.value.books);
      }
    });
    
    const response = {
      collection: {
        name: "Shona & Zimbabwe Literature",
        description: "Books in Shona language and about Zimbabwe culture and history",
        language: {
          code: "sn",
          name: "Shona",
          native: "chiShona"
        }
      },
      data: allBooks.slice(0, parseInt(limit)),
      meta: {
        total: allBooks.length,
        page: parseInt(page),
        sources_checked: ["internetarchive", "googlebooks", "openlibrary"],
        cached: false
      }
    };
    
    setCache(cacheKey, response);
    res.json(response);
  } catch (err) {
    console.error('Shona highlights error:', err);
    apiStats.errors++;
    res.status(500).json({ error: "Failed to fetch Shona books", details: err.message });
  }
});

app.get("/v1/search", requireApiKey, async (req, res) => {
  const { q, sources, language, author, limit = 20, page = 1 } = req.query;
  
  if (!q) {
    return res.status(400).json({ error: "Missing query parameter 'q'" });
  }
  
  const cacheKey = `search:${q}:${sources}:${language}:${page}`;
  const cached = getCached(cacheKey);
  if (cached) {
    return res.json({ ...cached, meta: { ...cached.meta, cached: true } });
  }
  
  try {
    const enabledSources = sources ? sources.split(',') : ['gutendex', 'openlibrary', 'archive', 'google'];
    const options = { language, author, page, limit };
    
    const searchPromises = [];
    if (enabledSources.includes('gutendex')) searchPromises.push(searchGutendex(q, options));
    if (enabledSources.includes('openlibrary')) searchPromises.push(searchOpenLibrary(q, options));
    if (enabledSources.includes('archive')) searchPromises.push(searchInternetArchive(q, options));
    if (enabledSources.includes('google')) searchPromises.push(searchGoogleBooks(q, options));
    
    const results = await Promise.allSettled(searchPromises);
    
    let allBooks = [];
    const sourceResults = {};
    
    results.forEach(result => {
      if (result.status === 'fulfilled' && result.value.success) {
        allBooks = allBooks.concat(result.value.books);
        sourceResults[result.value.source] = {
          success: true,
          count: result.value.books.length,
          total: result.value.total
        };
      } else if (result.status === 'fulfilled') {
        sourceResults[result.value.source] = {
          success: false,
          error: result.value.error
        };
      }
    });
    
    const response = {
      query: q,
      data: allBooks.slice(0, parseInt(limit) * 2),
      meta: {
        total: allBooks.length,
        page: parseInt(page),
        sources: sourceResults,
        cached: false
      }
    };
    
    setCache(cacheKey, response);
    res.json(response);
  } catch (err) {
    console.error('Search error:', err);
    apiStats.errors++;
    res.status(500).json({ error: "Search failed", details: err.message });
  }
});

app.get("/v1/search/advanced", requireApiKey, async (req, res) => {
  const { q, author, language, topic, region, year_from, year_to, sort, limit = 20, page = 1 } = req.query;
  
  if (!q && !author && !topic && !region) {
    return res.status(400).json({
      error: "At least one filter required",
      message: "Provide q, author, topic, or region parameter"
    });
  }
  
  const cacheKey = `adv:${JSON.stringify(req.query)}`;
  const cached = getCached(cacheKey);
  if (cached) {
    return res.json({ ...cached, meta: { ...cached.meta, cached: true } });
  }
  
  try {
    const searchTerms = [];
    if (q) searchTerms.push(q);
    if (author) searchTerms.push(author);
    if (topic) searchTerms.push(topic);
    
    const searchQuery = searchTerms.join(' ');
    const options = { language, page, limit, sort };
    
    const results = await Promise.allSettled([
      searchGutendex(searchQuery, options),
      searchOpenLibrary(searchQuery, options),
      searchInternetArchive(searchQuery, options),
      searchGoogleBooks(searchQuery, options)
    ]);
    
    let allBooks = [];
    results.forEach(result => {
      if (result.status === 'fulfilled' && result.value.success) {
        allBooks = allBooks.concat(result.value.books);
      }
    });
    
    if (year_from || year_to) {
      allBooks = allBooks.filter(book => {
        const year = book.first_publish_year || book.year || parseInt(book.published_date);
        if (!year) return true;
        if (year_from && year < parseInt(year_from)) return false;
        if (year_to && year > parseInt(year_to)) return false;
        return true;
      });
    }
    
    const response = {
      data: allBooks.slice(0, parseInt(limit)),
      meta: {
        total: allBooks.length,
        page: parseInt(page),
        filters: { q, author, language, topic, region, year_from, year_to, sort },
        cached: false
      }
    };
    
    setCache(cacheKey, response);
    res.json(response);
  } catch (err) {
    console.error('Advanced search error:', err);
    apiStats.errors++;
    res.status(500).json({ error: "Advanced search failed", details: err.message });
  }
});

app.get("/v1/sources/:source/search", requireApiKey, async (req, res) => {
  const { source } = req.params;
  const { q, language, author, limit = 20, page = 1 } = req.query;
  
  if (!q) {
    return res.status(400).json({ error: "Missing query parameter 'q'" });
  }
  
  const options = { language, author, page, limit };
  let result;
  
  try {
    switch (source.toLowerCase()) {
      case 'gutendex':
        result = await searchGutendex(q, options);
        break;
      case 'openlibrary':
        result = await searchOpenLibrary(q, options);
        break;
      case 'archive':
      case 'internetarchive':
        result = await searchInternetArchive(q, options);
        break;
      case 'google':
      case 'googlebooks':
        result = await searchGoogleBooks(q, options);
        break;
      default:
        return res.status(400).json({
          error: "Unknown source",
          available: ["gutendex", "openlibrary", "archive", "google"]
        });
    }
    
    if (!result.success) {
      return res.status(503).json({
        error: "Source unavailable",
        source,
        details: result.error
      });
    }
    
    res.json({
      source,
      query: q,
      data: result.books,
      meta: {
        total: result.total,
        page: parseInt(page)
      }
    });
  } catch (err) {
    console.error(`Source search error (${source}):`, err);
    apiStats.errors++;
    res.status(500).json({ error: "Search failed", details: err.message });
  }
});

app.get("/v1/trending", requireApiKey, async (req, res) => {
  const { limit = 20 } = req.query;
  
  const cacheKey = `trending:${limit}`;
  const cached = getCached(cacheKey);
  if (cached) {
    return res.json({ ...cached, meta: { ...cached.meta, cached: true } });
  }
  
  try {
    const result = await searchGutendex("", { sort: "popular", limit });
    
    if (!result.success) {
      return res.status(503).json({ error: "Failed to fetch trending books" });
    }
    
    const response = {
      data: result.books.slice(0, parseInt(limit)),
      meta: {
        total: result.books.length,
        description: "Most downloaded books",
        cached: false
      }
    };
    
    setCache(cacheKey, response);
    res.json(response);
  } catch (err) {
    console.error('Trending error:', err);
    apiStats.errors++;
    res.status(500).json({ error: "Failed to fetch trending books", details: err.message });
  }
});

app.get("/v1/random", requireApiKey, async (req, res) => {
  const { count = 5 } = req.query;
  const numBooks = Math.min(parseInt(count), 20);
  
  try {
    const randomIds = [];
    for (let i = 0; i < numBooks * 2; i++) {
      randomIds.push(Math.floor(Math.random() * 70000) + 1);
    }
    
    const promises = randomIds.slice(0, 10).map(id =>
      axios.get(`https://gutendex.com/books/${id}/`, { timeout: 10000 }).catch(() => null)
    );
    
    const responses = await Promise.all(promises);
    const books = responses
      .filter(r => r !== null && r.data)
      .slice(0, numBooks)
      .map(r => formatGutendexBook(r.data));
    
    res.json({
      data: books,
      meta: {
        requested: numBooks,
        returned: books.length,
        description: "Random books for discovery"
      }
    });
  } catch (err) {
    console.error('Random books error:', err);
    apiStats.errors++;
    res.status(500).json({ error: "Failed to fetch random books", details: err.message });
  }
});

app.get("/v1/collections", requireApiKey, (req, res) => {
  const collections = Object.entries(COLLECTIONS).map(([id, data]) => ({
    id,
    name: data.name,
    description: data.description,
    url: `/v1/collections/${id}`
  }));
  
  res.json({
    data: collections,
    meta: { total: collections.length }
  });
});

app.get("/v1/collections/:name", requireApiKey, async (req, res) => {
  const { name } = req.params;
  const { limit = 20, page = 1 } = req.query;
  
  if (!COLLECTIONS[name]) {
    return res.status(404).json({
      error: "Collection not found",
      available: Object.keys(COLLECTIONS)
    });
  }
  
  const collection = COLLECTIONS[name];
  const cacheKey = `collection:${name}:${page}`;
  const cached = getCached(cacheKey);
  if (cached) {
    return res.json({ ...cached, meta: { ...cached.meta, cached: true } });
  }
  
  try {
    const results = await Promise.allSettled([
      searchGutendex(collection.topic, { sort: "popular", page, limit }),
      searchInternetArchive(collection.topic, { page, limit }),
      searchGoogleBooks(collection.topic, { page, limit })
    ]);
    
    let allBooks = [];
    results.forEach(result => {
      if (result.status === 'fulfilled' && result.value.success) {
        allBooks = allBooks.concat(result.value.books);
      }
    });
    
    const response = {
      collection: {
        id: name,
        name: collection.name,
        description: collection.description
      },
      data: allBooks.slice(0, parseInt(limit)),
      meta: {
        total: allBooks.length,
        page: parseInt(page),
        cached: false
      }
    };
    
    setCache(cacheKey, response);
    res.json(response);
  } catch (err) {
    console.error('Collection error:', err);
    apiStats.errors++;
    res.status(500).json({ error: "Failed to fetch collection", details: err.message });
  }
});

app.get("/v1/subjects", requireApiKey, (req, res) => {
  const subjects = SUBJECTS.map(s => ({
    name: s,
    slug: s.toLowerCase().replace(/[^a-z0-9]+/g, '-'),
    url: `/v1/subjects/${encodeURIComponent(s)}/books`
  }));
  
  res.json({
    data: subjects,
    meta: { total: subjects.length }
  });
});

app.get("/v1/subjects/:name/books", requireApiKey, async (req, res) => {
  const { name } = req.params;
  const { limit = 20, page = 1 } = req.query;
  
  const cacheKey = `subject:${name}:${page}`;
  const cached = getCached(cacheKey);
  if (cached) {
    return res.json({ ...cached, meta: { ...cached.meta, cached: true } });
  }
  
  try {
    const results = await Promise.allSettled([
      searchGutendex(name, { topic: name, page, limit }),
      searchInternetArchive(name, { page, limit }),
      searchGoogleBooks(name, { page, limit })
    ]);
    
    let allBooks = [];
    results.forEach(result => {
      if (result.status === 'fulfilled' && result.value.success) {
        allBooks = allBooks.concat(result.value.books);
      }
    });
    
    const response = {
      subject: name,
      data: allBooks.slice(0, parseInt(limit)),
      meta: {
        total: allBooks.length,
        page: parseInt(page),
        cached: false
      }
    };
    
    setCache(cacheKey, response);
    res.json(response);
  } catch (err) {
    console.error('Subject books error:', err);
    apiStats.errors++;
    res.status(500).json({ error: "Failed to fetch books by subject", details: err.message });
  }
});

app.get("/v1/authors", requireApiKey, async (req, res) => {
  const { q, limit = 20 } = req.query;
  
  if (!q) {
    return res.status(400).json({ error: "Missing query parameter 'q'" });
  }
  
  const cacheKey = `authors:${q}`;
  const cached = getCached(cacheKey);
  if (cached) {
    return res.json({ ...cached, meta: { ...cached.meta, cached: true } });
  }
  
  try {
    const results = await Promise.allSettled([
      searchGutendex(q, { limit }),
      searchOpenLibrary(q, { author: q, limit }),
      searchGoogleBooks(q, { author: q, limit })
    ]);
    
    const authorMap = new Map();
    
    results.forEach(result => {
      if (result.status === 'fulfilled' && result.value.success) {
        result.value.books.forEach(book => {
          (book.authors || []).forEach(author => {
            const name = author.name;
            if (!name) return;
            
            const normalizedName = name.toLowerCase().trim();
            if (!normalizedName.includes(q.toLowerCase())) return;
            
            if (!authorMap.has(normalizedName)) {
              authorMap.set(normalizedName, {
                name: author.name,
                birth_year: author.birth_year,
                death_year: author.death_year,
                book_count: 0,
                sources: new Set(),
                sample_books: []
              });
            }
            
            const a = authorMap.get(normalizedName);
            a.book_count++;
            a.sources.add(book.source);
            if (a.sample_books.length < 5) {
              a.sample_books.push({
                id: book.id,
                title: book.title,
                cover: book.covers?.small
              });
            }
          });
        });
      }
    });
    
    const authors = Array.from(authorMap.values())
      .map(a => ({
        ...a,
        sources: Array.from(a.sources),
        profile_url: `/v1/authors/${encodeURIComponent(a.name)}/profile`,
        books_url: `/v1/authors/${encodeURIComponent(a.name)}/books`
      }))
      .sort((a, b) => b.book_count - a.book_count);
    
    const response = {
      query: q,
      data: authors.slice(0, parseInt(limit)),
      meta: {
        total: authors.length,
        cached: false
      }
    };
    
    setCache(cacheKey, response);
    res.json(response);
  } catch (err) {
    console.error('Authors search error:', err);
    apiStats.errors++;
    res.status(500).json({ error: "Author search failed", details: err.message });
  }
});

app.get("/v1/authors/:name/books", requireApiKey, async (req, res) => {
  const { name } = req.params;
  const { limit = 20, page = 1 } = req.query;
  
  const cacheKey = `author-books:${name}:${page}`;
  const cached = getCached(cacheKey);
  if (cached) {
    return res.json({ ...cached, meta: { ...cached.meta, cached: true } });
  }
  
  try {
    const results = await Promise.allSettled([
      searchGutendex(name, { page, limit }),
      searchOpenLibrary(name, { author: name, page, limit }),
      searchGoogleBooks(name, { author: name, page, limit })
    ]);
    
    let allBooks = [];
    results.forEach(result => {
      if (result.status === 'fulfilled' && result.value.success) {
        const authorBooks = result.value.books.filter(book =>
          book.authors?.some(a => 
            a.name?.toLowerCase().includes(name.toLowerCase())
          )
        );
        allBooks = allBooks.concat(authorBooks);
      }
    });
    
    const response = {
      author: name,
      data: allBooks.slice(0, parseInt(limit)),
      meta: {
        total: allBooks.length,
        page: parseInt(page),
        cached: false
      }
    };
    
    setCache(cacheKey, response);
    res.json(response);
  } catch (err) {
    console.error('Author books error:', err);
    apiStats.errors++;
    res.status(500).json({ error: "Failed to fetch author's books", details: err.message });
  }
});

app.get("/v1/authors/:name/profile", requireApiKey, async (req, res) => {
  const { name } = req.params;
  
  const cacheKey = `author-profile:${name}`;
  const cached = getCached(cacheKey);
  if (cached) {
    return res.json({ ...cached, meta: { ...cached.meta, cached: true } });
  }
  
  try {
    const gutResult = await searchGutendex(name, { limit: 30 });
    
    let authorInfo = null;
    let books = [];
    
    if (gutResult.success) {
      const authorBooks = gutResult.books.filter(book =>
        book.authors?.some(a => a.name?.toLowerCase().includes(name.toLowerCase()))
      );
      
      books = authorBooks;
      
      if (authorBooks.length > 0) {
        const author = authorBooks[0].authors.find(a => 
          a.name?.toLowerCase().includes(name.toLowerCase())
        );
        if (author) {
          authorInfo = {
            name: author.name,
            birth_year: author.birth_year,
            death_year: author.death_year
          };
        }
      }
    }
    
    const response = {
      author: authorInfo || { name },
      books: books.slice(0, 20),
      external_links: {
        wikipedia: `https://en.wikipedia.org/wiki/${encodeURIComponent(name.replace(/, /g, '_'))}`,
        openlibrary: `https://openlibrary.org/search/authors?q=${encodeURIComponent(name)}`,
        goodreads: `https://www.goodreads.com/search?q=${encodeURIComponent(name)}&search_type=authors`,
        amazon: `https://www.amazon.com/s?k=${encodeURIComponent(name)}&i=stripbooks`
      },
      meta: {
        book_count: books.length,
        cached: false
      }
    };
    
    setCache(cacheKey, response);
    res.json(response);
  } catch (err) {
    console.error('Author profile error:', err);
    apiStats.errors++;
    res.status(500).json({ error: "Failed to fetch author profile", details: err.message });
  }
});

app.get("/v1/books/:source/:id", requireApiKey, async (req, res) => {
  const { source, id } = req.params;
  
  const cacheKey = `book:${source}:${id}`;
  const cached = getCached(cacheKey);
  if (cached) {
    return res.json({ ...cached, meta: { ...cached.meta, cached: true } });
  }
  
  try {
    let book;
    
    switch (source.toLowerCase()) {
      case 'gutendex':
        const gutRes = await axios.get(`https://gutendex.com/books/${id}/`, { timeout: 15000 });
        book = formatGutendexBook(gutRes.data);
        break;
        
      case 'openlibrary':
        const olRes = await axios.get(`https://openlibrary.org/works/${id}.json`, { timeout: 15000 });
        const searchRes = await axios.get(`https://openlibrary.org/search.json?q=key:/works/${id}&limit=1`, { timeout: 15000 });
        book = searchRes.data.docs[0] ? formatOpenLibraryBook(searchRes.data.docs[0]) : { id, title: olRes.data.title, source: 'openlibrary' };
        break;
        
      case 'archive':
        const archiveRes = await axios.get(`https://archive.org/metadata/${id}`, { timeout: 15000 });
        book = formatInternetArchiveBook(archiveRes.data.metadata);
        break;
        
      case 'google':
        const googleRes = await axios.get(`https://www.googleapis.com/books/v1/volumes/${id}`, { timeout: 15000 });
        book = formatGoogleBook(googleRes.data);
        break;
        
      default:
        return res.status(400).json({
          error: "Unknown source",
          available: ["gutendex", "openlibrary", "archive", "google"]
        });
    }
    
    const response = {
      data: book,
      meta: { cached: false }
    };
    
    setCache(cacheKey, response);
    res.json(response);
  } catch (err) {
    console.error('Book details error:', err);
    apiStats.errors++;
    if (err.response?.status === 404) {
      res.status(404).json({ error: "Book not found" });
    } else {
      res.status(500).json({ error: "Failed to fetch book details", details: err.message });
    }
  }
});

app.get("/v1/books/:source/:id/covers", requireApiKey, async (req, res) => {
  const { source, id } = req.params;
  
  try {
    let covers = {};
    
    switch (source.toLowerCase()) {
      case 'gutendex':
        const gutRes = await axios.get(`https://gutendex.com/books/${id}/`, { timeout: 15000 });
        const cover = gutRes.data.formats["image/jpeg"];
        covers = {
          small: cover,
          medium: cover,
          large: cover
        };
        break;
        
      case 'openlibrary':
        const searchRes = await axios.get(`https://openlibrary.org/search.json?q=key:/works/${id}&limit=1`, { timeout: 15000 });
        const coverId = searchRes.data.docs[0]?.cover_i;
        if (coverId) {
          covers = {
            small: `https://covers.openlibrary.org/b/id/${coverId}-S.jpg`,
            medium: `https://covers.openlibrary.org/b/id/${coverId}-M.jpg`,
            large: `https://covers.openlibrary.org/b/id/${coverId}-L.jpg`
          };
        }
        break;
        
      case 'archive':
        covers = {
          small: `https://archive.org/services/img/${id}`,
          medium: `https://archive.org/services/img/${id}`,
          large: `https://archive.org/services/img/${id}`
        };
        break;
        
      case 'google':
        const googleRes = await axios.get(`https://www.googleapis.com/books/v1/volumes/${id}`, { timeout: 15000 });
        const imageLinks = googleRes.data.volumeInfo?.imageLinks || {};
        covers = {
          small: imageLinks.smallThumbnail,
          medium: imageLinks.thumbnail,
          large: imageLinks.large || imageLinks.thumbnail?.replace('zoom=1', 'zoom=2')
        };
        break;
        
      default:
        return res.status(400).json({ error: "Unknown source" });
    }
    
    res.json({
      book_id: `${source}:${id}`,
      covers,
      meta: {
        source,
        formats: Object.keys(covers).filter(k => covers[k])
      }
    });
  } catch (err) {
    console.error('Covers error:', err);
    apiStats.errors++;
    res.status(500).json({ error: "Failed to fetch covers", details: err.message });
  }
});

app.get("/v1/books/:source/:id/formats", requireApiKey, async (req, res) => {
  const { source, id } = req.params;
  
  try {
    let formats = {};
    let downloadUrls = {};
    
    switch (source.toLowerCase()) {
      case 'gutendex':
        const gutRes = await axios.get(`https://gutendex.com/books/${id}/`, { timeout: 15000 });
        const f = gutRes.data.formats;
        formats = {
          epub: { available: !!f["application/epub+zip"], url: f["application/epub+zip"] },
          pdf: { available: true, url: `/v1/download/gutendex/${id}?format=pdf` },
          txt: { available: !!(f["text/plain"] || f["text/plain; charset=us-ascii"]), url: f["text/plain"] || f["text/plain; charset=us-ascii"] },
          html: { available: !!f["text/html"], url: f["text/html"] },
          mobi: { available: !!f["application/x-mobipocket-ebook"], url: f["application/x-mobipocket-ebook"] }
        };
        break;
        
      case 'archive':
        formats = {
          epub: { available: true, url: `https://archive.org/download/${id}/${id}.epub` },
          pdf: { available: true, url: `https://archive.org/download/${id}/${id}.pdf` },
          txt: { available: true, url: `https://archive.org/download/${id}/${id}_djvu.txt` },
          mobi: { available: true, url: `https://archive.org/download/${id}/${id}.mobi` }
        };
        break;
        
      case 'google':
        const googleRes = await axios.get(`https://www.googleapis.com/books/v1/volumes/${id}`, { timeout: 15000 });
        const access = googleRes.data.accessInfo || {};
        formats = {
          epub: { available: access.epub?.isAvailable, url: access.epub?.downloadLink },
          pdf: { available: access.pdf?.isAvailable, url: access.pdf?.downloadLink },
          webReader: { available: true, url: googleRes.data.volumeInfo?.previewLink }
        };
        break;
        
      default:
        return res.status(400).json({ error: "Unknown source" });
    }
    
    res.json({
      book_id: `${source}:${id}`,
      formats,
      download_endpoint: `/v1/download/${source}/${id}`,
      meta: {
        source,
        available_formats: Object.keys(formats).filter(k => formats[k].available)
      }
    });
  } catch (err) {
    console.error('Formats error:', err);
    apiStats.errors++;
    res.status(500).json({ error: "Failed to fetch formats", details: err.message });
  }
});


// Read Online Links
app.get("/v1/books/:source/:id/read-online", requireApiKey, async (req, res) => {
  const { source, id } = req.params;
  
  try {
    let readOnlineLinks = [];
    
    if (source === 'gutendex') {
      readOnlineLinks = [
        { platform: "Project Gutenberg", url: `https://www.gutenberg.org/cache/epub/${id}/pg${id}-images.html` },
        { platform: "Gutenberg Text", url: `https://www.gutenberg.org/ebooks/${id}.txt.utf-8` }
      ];
    } else if (source === 'openlibrary') {
      readOnlineLinks = [
        { platform: "Open Library", url: `https://openlibrary.org${id}` }
      ];
    } else if (source === 'archive') {
      readOnlineLinks = [
        { platform: "Internet Archive", url: `https://archive.org/stream/${id}` }
      ];
    }
    
    res.json({
      book_id: `${source}:${id}`,
      read_online_platforms: readOnlineLinks,
      note: "Read online directly from source"
    });
  } catch (err) {
    apiStats.errors++;
    res.status(500).json({ error: "Failed to fetch read online links" });
  }
});


// Download History (In-memory - for this session)
const downloadHistory = [];
app.post("/v1/downloads/log", requireApiKey, (req, res) => {
  const { book_id, format, status } = req.body;
  
  downloadHistory.push({
    book_id,
    format: format || 'epub',
    status: status || 'completed',
    timestamp: new Date().toISOString()
  });
  
  res.json({
    logged: true,
    download: { book_id, format, status, timestamp: new Date().toISOString() }
  });
});

app.get("/v1/downloads/history", requireApiKey, (req, res) => {
  const { limit = 20 } = req.query;
  
  res.json({
    data: downloadHistory.slice(-parseInt(limit)).reverse(),
    meta: {
      total: downloadHistory.length,
      formats_used: [...new Set(downloadHistory.map(d => d.format))]
    }
  });
});

// MOBI Conversion Info
app.get("/v1/download/:source/:id/mobi-info", requireApiKey, (req, res) => {
  const { source, id } = req.params;
  
  res.json({
    book_id: `${source}:${id}`,
    mobi_format: {
      name: "Mobipocket eBook Format",
      extension: ".mobi",
      devices: ["Amazon Kindle", "Other eReaders"],
      download_endpoint: `/v1/download/${source}/${id}?format=mobi`,
      supported_sources: ["gutendex", "archive"],
      description: "MOBI is a popular eBook format compatible with Kindle devices and other readers"
    }
  });
});

// Search by ISBN
app.get("/v1/search/isbn/:isbn", requireApiKey, async (req, res) => {
  const { isbn } = req.params;
  
  try {
    const results = await Promise.allSettled([
      searchGoogleBooks(`ISBN:${isbn}`),
      searchOpenLibrary(isbn)
    ]);
    
    let allBooks = [];
    results.forEach(result => {
      if (result.status === 'fulfilled' && result.value.success) {
        allBooks = allBooks.concat(result.value.books);
      }
    });
    
    res.json({
      isbn,
      data: allBooks,
      meta: { total: allBooks.length }
    });
  } catch (err) {
    apiStats.errors++;
    res.status(500).json({ error: "ISBN search failed" });
  }
});

// Collections Details
app.get("/v1/collections/:name", requireApiKey, async (req, res) => {
  const { name } = req.params;
  const collection = COLLECTIONS[name.toLowerCase()];
  
  if (!collection) {
    return res.status(404).json({
      error: "Collection not found",
      available: Object.keys(COLLECTIONS)
    });
  }
  
  try {
    const result = await searchGutendex(collection.topic, { sort: 'popular', limit: 30 });
    
    res.json({
      collection: { id: name, ...collection },
      data: result.success ? result.books : [],
      meta: {
        total: result.success ? result.books.length : 0,
        description: collection.description
      }
    });
  } catch (err) {
    apiStats.errors++;
    res.status(500).json({ error: "Failed to load collection" });
  }
});

// Download with intelligent multi-source fallbacks
async function tryDownloadFromSource(sourceType, bookId, format, filename, mimeType) {
  const formatMap = {
    'epub3': '.epub3.images',
    'epub': '.epub.images',
    'epub-noimages': '.epub.noimages',
    'kindle': '.kf8.images',
    'mobi': '.kindle.images'
  };

  const ext = formatMap[format] || '.epub';

  if (sourceType === 'gutendex') {
    const urls = [
      `https://www.gutenberg.org/ebooks/${bookId}${ext}`,
      `https://www.gutenberg.org/cache/epub/${bookId}/pg${bookId}${ext}`,
      `https://www.gutenberg.org/files/${bookId}/${bookId}-0${ext}`,
      `https://www.gutenberg.org/files/${bookId}/${bookId}${ext}`
    ];

    for (const url of urls) {
      try {
        const response = await axios.get(url, {
          responseType: 'stream',
          headers: { 'User-Agent': 'Mozilla/5.0' },
          timeout: 15000,
          validateStatus: () => true
        });

        if (response.status === 200 && response.data) {
          const contentLength = response.headers['content-length'];
          if (!contentLength || parseInt(contentLength) > 500) {
            return { stream: response.data, mimeType };
          }
        }
      } catch (e) {
        continue;
      }
    }
  } else if (sourceType === 'archive') {
    const formatUrls = {
      epub3: `https://archive.org/download/${bookId}/${bookId}.epub`,
      epub: `https://archive.org/download/${bookId}/${bookId}.epub`,
      'epub-noimages': `https://archive.org/download/${bookId}/${bookId}.epub`,
      kindle: `https://archive.org/download/${bookId}/${bookId}.epub`,
      mobi: `https://archive.org/download/${bookId}/${bookId}.epub`
    };

    const url = formatUrls[format];
    if (url) {
      try {
        const response = await axios.get(url, {
          responseType: 'stream',
          timeout: 15000,
          validateStatus: () => true
        });

        if (response.status === 200 && response.data) {
          return { stream: response.data, mimeType: 'application/epub+zip' };
        }
      } catch (e) {
        return null;
      }
    }
  }

  return null;
}

app.get("/v1/download/:source/:id", requireApiKey, async (req, res) => {
  const { source, id } = req.params;
  const format = (req.query.format || "epub3").toLowerCase();

  console.log(`Download request: ${source}/${id} format:${format}`);

  try {
    const bookId = id.replace("gutendex:", "").replace("archive:", "");
    const mimeTypeMap = {
      'epub3': 'application/epub+zip',
      'epub': 'application/epub+zip',
      'epub-noimages': 'application/epub+zip',
      'kindle': 'application/vnd.amazon.ebook',
      'mobi': 'application/x-mobipocket-ebook'
    };

    const filename = `book-${bookId}.${format === 'kindle' ? 'azw3' : format === 'mobi' ? 'mobi' : 'epub'}`;
    const mimeType = mimeTypeMap[format] || 'application/octet-stream';

    // Try primary source first
    let result = await tryDownloadFromSource(source, bookId, format, filename, mimeType);

    // Fallback to Internet Archive
    if (!result && source === 'gutendex') {
      console.log(`Gutendex failed, trying Internet Archive...`);
      result = await tryDownloadFromSource('archive', bookId, format, filename, mimeType);
    }

    // Fallback to Gutendex if Archive fails
    if (!result && source === 'archive') {
      console.log(`Archive failed, trying Gutendex...`);
      result = await tryDownloadFromSource('gutendex', bookId, format, filename, mimeType);
    }

    if (result && result.stream) {
      res.setHeader('Content-Type', result.mimeType);
      res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
      res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
      result.stream.pipe(res);
      console.log(`Download started: ${filename}`);
      return;
    }

    res.status(404).json({ error: `Book ${bookId} not available in ${format} format` });
  } catch (err) {
    console.error('Download error:', err.message);
    apiStats.errors++;
    if (!res.headersSent) {
      res.status(500).json({ error: "Download failed: " + err.message });
    }
  }
});

app.use((req, res) => {
  res.status(404).json({
    error: "Endpoint not found",
    message: `${req.method} ${req.path} does not exist`,
    documentation: "/v1/docs"
  });
});

app.use((error, req, res, next) => {
  console.error('Unhandled error:', error);
  apiStats.errors++;
  res.status(500).json({
    error: 'Internal server error',
    details: error.message,
    documentation: "/v1/docs"
  });
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`\n========================================`);
  console.log(`  Global Book API v3.0`);
  console.log(`  Running at http://0.0.0.0:${PORT}`);
  console.log(`  API Key: ${API_KEY}`);
  console.log(`  Sources: Gutendex, OpenLibrary, Internet Archive, Google Books`);
  console.log(`  Languages: ${Object.keys(LANGUAGES).length} supported`);
  console.log(`  Regions: ${Object.keys(REGIONS).length} supported`);
  console.log(`  Rate Limit: ${RATE_LIMIT} req/min`);
  console.log(`========================================\n`);
  
  const tempDir = path.join(__dirname, 'temp');
  if (!fs.existsSync(tempDir)) {
    fs.mkdirSync(tempDir, { recursive: true });
  }
});
