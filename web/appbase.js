
let currentIndex = 0;

const imageElement = document.getElementById("image");
const transcriptionContainer = document.getElementById("transcription");
const highlightElement = document.getElementById("highlight");

function loadImage(index) {
    const { src, transcription } = images[index];
    const sugerencias = suggested[index]

    // Set the image source
    imageElement.src = src;

    // Clear existing transcription
    transcriptionContainer.innerHTML = "";

    // Add new transcription lines
    transcription.forEach((line, i) => {
        const lineElement = document.createElement("div");

	const linkElement = document.createElement("a");
	linkElement.href = `https://cantusindex.org/id/${line.cantusid}`;
	linkElement.textContent = line.cantusid;
	linkElement.target = "_blank"; // new tab
	linkElement.rel = "noopener noreferrer"; // security

        // lineElement.textContent = line.text;
	lineElement.appendChild(document.createTextNode("("));
	lineElement.appendChild(linkElement);
	lineElement.appendChild(document.createTextNode(") "));
	lineElement.appendChild(document.createTextNode(line.text));
	const costElement = document.createElement("span");
	costElement.textContent = line.cost;
	if (line.cost > 0.5) {
    		costElement.style.color = "darkgreen";
	} else {
    		costElement.style.color = "lightgreen";
		lineElement.style.backgroundColor = "lightcoral"
	}
	lineElement.appendChild(document.createTextNode(" ("));
	lineElement.appendChild(costElement);
	lineElement.appendChild(document.createTextNode(") "));
	
	const lineBreak = document.createElement("br");
	lineElement.appendChild(lineBreak);
	const additionalText = document.createElement("span");
	additionalText.textContent = line.melody;
	additionalText.style.fontFamily = "Courier, monospace";
	lineElement.appendChild(additionalText);

        lineElement.style.marginBottom = "10px";
        lineElement.addEventListener("mouseover", () => showHighlight(line.height, line.heightmax));
        lineElement.addEventListener("mouseout", () => hideHighlight());
        transcriptionContainer.appendChild(lineElement);
    });

    // Add new suggestion
    sugerencias.forEach(entry => {
  	const lineElement = document.createElement("div");
	const linkElement = document.createElement("a");
	linkElement.href = `https://cantusindex.org/id/${entry}`;
	linkElement.textContent = entry;
	linkElement.target = "_blank"; // new tab
	linkElement.rel = "noopener noreferrer"; // security
	lineElement.appendChild(linkElement);
	transcriptionContainer.appendChild(lineElement);
    });

}

function showHighlight(height, heightmax) {
    const imgRect = imageElement.getBoundingClientRect();
    const highlightHeight = height * imgRect.height;

    highlightElement.style.top = `${imgRect.top + window.scrollY + highlightHeight}px`;
    //highlightElement.style.height = "20px"; // Adjust for line thickness
    highlightElement.style.height = `${imgRect.height * (heightmax-height) + 40}px`;; // Adjust for line thickness
    highlightElement.style.display = "block";
}

function hideHighlight() {
    highlightElement.style.display = "none";
}

document.getElementById("prev").addEventListener("click", () => {
    if (currentIndex > 0) {
        currentIndex--;
	const numberElement = document.getElementById("number");
	numberElement.textContent = currentIndex;
        loadImage(currentIndex);
    }
});

document.getElementById("next").addEventListener("click", () => {
    if (currentIndex < images.length - 1) {
        currentIndex++;
	const numberElement = document.getElementById("number");
	numberElement.textContent = currentIndex;
        loadImage(currentIndex);
    }
});

// Load the first image
loadImage(currentIndex);
