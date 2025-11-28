require("dotenv").config();
const express = require("express");
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const PDFDocument = require("pdfkit");
const Epub = require("epub");
const app = express();

const PORT = process.env.PORT || 4000;
const API_KEY = process.env.API_KEY || "my-api-key-123";

// Middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// --- API Key middleware ---
function requireApiKey(req, res, next) {
  const key = req.header("x-api-key");
  if (!key || key !== API_KEY) {
    return res.status(401).json({ error: "Invalid API key" });
  }
  next();
}

// Helper function to download file
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

// Helper function to extract text from EPUB
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
            // Clean up HTML tags and extract text
            const cleanText = text.toString()
              .replace(/<[^>]*>/g, ' ') // Remove HTML tags
              .replace(/\s+/g, ' ')     // Normalize whitespace
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

// --- Search endpoint ---
app.get("/v1/search", requireApiKey, async (req, res) => {
  const q = req.query.q;
  if (!q) return res.status(400).json({ error: "Missing query parameter 'q'" });

  try {
    console.log(`Searching for: ${q}`);
    
    const gutRes = await axios.get(`https://gutendex.com/books/?search=${encodeURIComponent(q)}`);
    const gutBooks = gutRes.data.results.map(b => ({
      id: `gutendex:${b.id}`,
      source: "gutendex",
      title: b.title,
      authors: b.authors.map(a => a.name),
      subjects: b.subjects,
      formats: {
        txt: b.formats["text/plain; charset=us-ascii"],
        epub: b.formats["application/epub+zip"],
        mobi: b.formats["application/x-mobipocket-ebook"],
        pdf: "available"
      },
      cover: b.formats["image/jpeg"]
    }));

    const olRes = await axios.get(`https://openlibrary.org/search.json?q=${encodeURIComponent(q)}`);
    const olBooks = olRes.data.docs.slice(0, 10).map(b => ({
      id: `ol:${b.key}`,
      source: "openlibrary",
      title: b.title,
      authors: b.author_name || [],
      cover: b.cover_i ? `https://covers.openlibrary.org/b/id/${b.cover_i}-M.jpg` : null,
      formats: {}
    }));

    res.json({ 
      total: gutBooks.length + olBooks.length, 
      gutendex_results: gutBooks.length,
      openlibrary_results: olBooks.length,
      items: [...gutBooks, ...olBooks] 
    });
  } catch (err) {
    console.error('Search error:', err);
    res.status(500).json({ error: "Search failed", details: err.message });
  }
});

// --- Book details endpoint ---
app.get("/v1/books/:source/:id", requireApiKey, async (req, res) => {
  const { source, id } = req.params;
  console.log(`Book details requested: ${source}/${id}`);
  
  try {
    if (source === "gutendex") {
      const bId = id.replace("gutendex:", "");
      const gutRes = await axios.get(`https://gutendex.com/books/${bId}/`);
      const b = gutRes.data;
      
      const bookDetails = {
        id: `gutendex:${b.id}`,
        title: b.title,
        authors: b.authors.map(a => a.name),
        subjects: b.subjects || [],
        languages: b.languages || [],
        download_count: b.download_count,
        formats: {
          txt: b.formats["text/plain; charset=us-ascii"] || b.formats["text/plain"],
          epub: b.formats["application/epub+zip"],
          mobi: b.formats["application/x-mobipocket-ebook"],
          pdf: "available"
        },
        cover: b.formats["image/jpeg"]
      };
      
      res.json(bookDetails);
    } else {
      res.status(400).json({ error: "Unknown source. Only 'gutendex' supported for details." });
    }
  } catch (err) {
    console.error('Book details error:', err);
    res.status(500).json({ error: "Failed to get book details", details: err.message });
  }
});

// --- Download endpoint ---
app.get("/v1/download/:source/:id", requireApiKey, async (req, res) => {
  const { source, id } = req.params;
  const format = (req.query.format || "epub").toLowerCase();
  
  console.log(`Download request: ${source}/${id} format:${format}`);

  try {
    if (source !== "gutendex") {
      return res.status(400).json({ error: "Only gutendex source supported for downloads" });
    }

    const bId = id.replace("gutendex:", "");
    const filename = `book-${bId}.${format}`;

    if (format === "pdf") {
      // PDF generation from EPUB
      res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
      res.setHeader('Content-Type', 'application/pdf');
      
      try {
        // Try multiple possible EPUB URLs
        const possibleUrls = [
          `https://www.gutenberg.org/ebooks/${bId}.epub.images`,
          `https://www.gutenberg.org/ebooks/${bId}.epub.noimages`,
          `https://www.gutenberg.org/files/${bId}/${bId}-0.epub`,
          `https://www.gutenberg.org/files/${bId}/${bId}.epub`
        ];

        let epubPath = null;
        let epubDownloaded = false;

        // Ensure temp directory exists
        const tempDir = path.join(__dirname, 'temp');
        if (!fs.existsSync(tempDir)) {
          fs.mkdirSync(tempDir, { recursive: true });
        }

        // Try each URL until one works
        for (const url of possibleUrls) {
          try {
            epubPath = path.join(tempDir, `${bId}.epub`);
            console.log(`Trying to download EPUB from: ${url}`);
            await downloadFile(url, epubPath);
            
            // Check if file was actually downloaded and has content
            const stats = fs.statSync(epubPath);
            if (stats.size > 1000) { // Reasonable minimum size for EPUB
              epubDownloaded = true;
              console.log(`Successfully downloaded EPUB (${stats.size} bytes)`);
              break;
            } else {
              console.log(`File too small, likely error page: ${stats.size} bytes`);
              fs.unlinkSync(epubPath);
            }
          } catch (error) {
            console.log(`Failed to download from ${url}: ${error.message}`);
            continue;
          }
        }

        if (!epubDownloaded) {
          return res.status(404).json({ error: "EPUB file not found for PDF conversion" });
        }

        // Extract text from EPUB and generate PDF
        console.log('Extracting text from EPUB...');
        const bookText = await extractTextFromEpub(epubPath);
        
        // Clean up temporary EPUB file
        fs.unlinkSync(epubPath);

        if (!bookText || bookText.trim().length === 0) {
          return res.status(500).json({ error: "No readable content found in EPUB" });
        }

        console.log('Generating PDF...');
        
        // Create PDF
        const doc = new PDFDocument();
        doc.pipe(res);

        // Add title
        doc.fontSize(20).text(`Book ID: ${bId}`, { align: 'center' });
        doc.moveDown();

        // Split text into manageable chunks and add to PDF
        const chunks = bookText.match(/[\s\S]{1,2000}/g) || [];
        console.log(`Adding ${chunks.length} text chunks to PDF...`);
        
        for (let i = 0; i < Math.min(chunks.length, 100); i++) { // Limit to first 100 chunks
          if (i > 0 && i % 5 === 0) {
            doc.addPage();
          }
          doc.fontSize(10).text(chunks[i], {
            align: 'left',
            width: 500,
            indent: 20
          });
          doc.moveDown(0.5);
        }

        doc.end();
        console.log('PDF generated successfully');

      } catch (error) {
        console.error('PDF generation error:', error);
        // If PDF generation fails, try to send error as JSON
        if (!res.headersSent) {
          res.status(500).json({ 
            error: "PDF generation failed", 
            details: error.message 
          });
        }
      }

    } else {
      // Direct download for EPUB/TXT/MOBI
      const extMap = { 
        epub: "epub", 
        txt: "txt", 
        mobi: "mobi" 
      };

      if (!extMap[format]) {
        return res.status(400).json({ error: "Unsupported format. Use: epub, txt, mobi, or pdf" });
      }

      // Try multiple possible file URLs
      const possibleUrls = [
        `https://www.gutenberg.org/files/${bId}/${bId}-0.${extMap[format]}`,
        `https://www.gutenberg.org/files/${bId}/${bId}.${extMap[format]}`,
        `https://www.gutenberg.org/ebooks/${bId}.${extMap[format]}.images`,
        `https://www.gutenberg.org/ebooks/${bId}.${extMap[format]}.noimages`,
        `https://www.gutenberg.org/cache/epub/${bId}/pg${bId}.${extMap[format]}`
      ];

      let downloaded = false;

      for (const url of possibleUrls) {
        try {
          console.log(`Trying to download ${format} from: ${url}`);
          const response = await axios({
            url: url,
            method: "GET",
            responseType: "stream",
            headers: {
              'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            },
            timeout: 30000,
            validateStatus: function (status) {
              return status === 200; // Only accept 200 status
            }
          });

          // Check if we got a reasonable file size (not an error page)
          const contentLength = response.headers['content-length'];
          if (contentLength && parseInt(contentLength) < 1000) {
            console.log(`File too small, likely error: ${contentLength} bytes`);
            continue;
          }

          // Set appropriate headers
          const contentTypeMap = {
            epub: 'application/epub+zip',
            txt: 'text/plain; charset=utf-8',
            mobi: 'application/x-mobipocket-ebook'
          };
          
          res.setHeader('Content-Type', contentTypeMap[format] || 'application/octet-stream');
          res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
          res.setHeader('Content-Length', contentLength || 'unknown');
          
          console.log(`Streaming ${format} file...`);
          response.data.pipe(res);
          downloaded = true;
          break;

        } catch (error) {
          console.log(`Failed to download from ${url}: ${error.message}`);
          continue;
        }
      }

      if (!downloaded) {
        res.status(404).json({ error: `File not found in ${format} format for book ${bId}` });
      }
    }

  } catch (err) {
    console.error('Download endpoint error:', err);
    if (!res.headersSent) {
      res.status(500).json({ 
        error: "Download failed", 
        details: err.message 
      });
    }
  }
});

// Root endpoint
app.get("/", (req, res) => {
  res.json({ 
    message: "Book API Server", 
    version: "1.0",
    endpoints: {
      search: "/v1/search?q=query",
      book_details: "/v1/books/gutendex:id", 
      download: "/v1/download/gutendex:id?format=epub|txt|mobi|pdf"
    }
  });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({ error: "Endpoint not found" });
});

// Error handling middleware
app.use((error, req, res, next) => {
  console.error('Unhandled error:', error);
  res.status(500).json({ error: 'Internal server error', details: error.message });
});

// --- Start server ---
app.listen(PORT, "0.0.0.0", () => {
  console.log(`Multi-source Book API running at http://0.0.0.0:${PORT}`);
  console.log(`API Key: ${API_KEY}`);
  
  // Create temp directory if it doesn't exist
  const tempDir = path.join(__dirname, 'temp');
  if (!fs.existsSync(tempDir)) {
    fs.mkdirSync(tempDir, { recursive: true });
  }
});
