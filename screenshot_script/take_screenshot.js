const puppeteer = require('puppeteer');

(async () => {
  const browser = await puppeteer.launch({ headless: true });
  const page = await browser.newPage();
  
  // Set resolution to 16:9 full HD
  await page.setViewport({ width: 1920, height: 1080 });
  
  // Enable request interception to mock API
  await page.setRequestInterception(true);
  
  page.on('request', request => {
    const url = request.url();
    if (url.includes('localhost:8000/engines')) {
      request.respond({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          web: ["duckduckgo", "google", "bing", "searxng"],
          images: ["google_images", "bing_images"],
          files: ["duckduckgo", "searxng"]
        })
      });
    } else if (url.includes('localhost:8000/search')) {
      // Check the URL to mock the right category
      const urlObj = new URL(url);
      const category = urlObj.searchParams.get('categories') || 'general';
      const q = urlObj.searchParams.get('q') || '';
      
      let results = [];
      
      if (category === 'images') {
        results = [
          {
            title: "Quantum Computer Core",
            url: "https://example.com/quantum1.jpg",
            img_src: "https://images.unsplash.com/photo-1635070041078-e363dbe005cb",
            thumbnail: "https://images.unsplash.com/photo-1635070041078-e363dbe005cb?w=400",
            source: "Unsplash",
            engine: "Google Images"
          },
          {
            title: "Abstract Quantum Technology",
            url: "https://example.com/quantum2.jpg",
            img_src: "https://images.unsplash.com/photo-1518770660439-4636190af475",
            thumbnail: "https://images.unsplash.com/photo-1518770660439-4636190af475?w=400",
            source: "Unsplash",
            engine: "Bing Images"
          },
          {
            title: "Futuristic Data Center",
            url: "https://example.com/quantum3.jpg",
            img_src: "https://images.unsplash.com/photo-1550751827-4bd374c3f58b",
            thumbnail: "https://images.unsplash.com/photo-1550751827-4bd374c3f58b?w=400",
            source: "Unsplash",
            engine: "Google Images"
          }
        ];
      } else if (category === 'files') {
        results = [
          {
            title: "Quantum Supremacy Using a Programmable Superconducting Processor.pdf",
            url: "https://example.com/files/quantum_supremacy.pdf",
            snippet: "The promise of quantum computers is that certain computational tasks might be executed exponentially faster on a quantum processor than on a classical processor...",
            source: "Nature",
            filetype: "pdf",
            engine: "DuckDuckGo"
          },
          {
            title: "Introduction to Quantum Computing Algorithms.pptx",
            url: "https://example.com/files/intro_qc.pptx",
            snippet: "A comprehensive slide deck introducing basic quantum gates, Shor's algorithm, and Grover's search algorithm for university students.",
            source: "MIT OpenCourseWare",
            filetype: "pptx",
            engine: "Bing"
          },
          {
            title: "Qiskit Research Paper Draft Final.docx",
            url: "https://example.com/files/qiskit_draft.docx",
            snippet: "Working draft paper on optimization of quantum circuits using IBM's Qiskit library. Includes experimental results from a 50-qubit processor.",
            source: "Arxiv",
            filetype: "docx",
            engine: "Searxng"
          }
        ];
      } else {
        // Default (web/general)
        results = [
          {
            title: "Quantum computing - Wikipedia",
            url: "https://en.wikipedia.org/wiki/Quantum_computing",
            snippet: "Quantum computing is a rapidly-emerging technology that harnesses the laws of quantum mechanics to solve problems too complex for classical computers.",
            source: "Wikipedia",
            engine: "Google"
          },
          {
            title: "What Is a Quantum Computer? | IBM",
            url: "https://www.ibm.com/topics/quantum-computing",
            snippet: "Quantum computing is a rapidly-emerging technology that harnesses the laws of quantum mechanics to solve problems too complex for classical computers...",
            source: "IBM",
            engine: "DuckDuckGo"
          },
          {
            title: "Google Quantum AI",
            url: "https://quantumai.google/",
            snippet: "Advancing the state of the art in quantum computing and developing tools for researchers to operate beyond classical limits.",
            source: "Google Quantum AI",
            engine: "Bing"
          }
        ];
      }
      
      request.respond({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          query: q,
          total_results: 1420500,
          elapsed_seconds: 0.38,
          engines_queried: ["google", "duckduckgo", "bing", "yacy"],
          engine_stats: {
            "google": { count: 12, status: "ok" },
            "duckduckgo": { count: 8, status: "ok" },
            "bing": { count: 10, status: "ok" },
            "yacy": { count: 0, status: "error", error: "Connection timeout" },
            "searxng": { count: 15, status: "ok" }
          },
          _cached: false,
          results: results
        })
      });
    } else {
      request.continue();
    }
  });

  console.log("Navigating to home page...");
  await page.goto('http://localhost:3000', { waitUntil: 'networkidle0' });
  
  // Wait a bit to ensure animations finish
  await new Promise(r => setTimeout(r, 1000));
  
  console.log("Performing search for Quantum Computing...");
  await page.type('input[type="text"]', 'Quantum Computing Breakthroughs');
  await page.keyboard.press('Enter');
  
  // Wait for web results to load
  await new Promise(r => setTimeout(r, 4000));
  
  console.log("Saving search.png (Web results, no diagnostics)...");
  await page.screenshot({ path: '../screenshots/search.png' });
  
  console.log("Opening diagnostics...");
  // Look for the "Diagnostics" button text
  const diagButtons = await page.$$('button');
  for (let btn of diagButtons) {
    const text = await page.evaluate(el => el.textContent, btn);
    if (text && text.includes('Diagnostics')) {
      await btn.click();
      break;
    }
  }
  
  await new Promise(r => setTimeout(r, 1000));
  console.log("Saving search-diagnose.png...");
  await page.screenshot({ path: '../screenshots/search-diagnose.png' });
  
  console.log("Switching to Files tab...");
  // Find and click the "Files" tab
  const tabs = await page.$$('button');
  for (let tab of tabs) {
    const text = await page.evaluate(el => el.textContent, tab);
    if (text === 'Files') {
      await tab.click();
      break;
    }
  }
  
  await new Promise(r => setTimeout(r, 3000));
  console.log("Saving search-files.png...");
  await page.screenshot({ path: '../screenshots/search-files.png' });

  console.log("Switching to Images tab...");
  const tabs2 = await page.$$('button');
  for (let tab of tabs2) {
    const text = await page.evaluate(el => el.textContent, tab);
    if (text === 'Images') {
      await tab.click();
      break;
    }
  }
  
  await new Promise(r => setTimeout(r, 4000));
  console.log("Saving search-images.png...");
  await page.screenshot({ path: '../screenshots/search-images.png' });
  
  await browser.close();
  console.log("All done!");
})();
