// @ts-nocheck
/**
 * Virtual scrolling implementation for large lists.
 * Only renders visible items plus overscan buffer, recycling DOM nodes.
 */

interface VirtualListOptions {
  itemCount: number;
  estimatedItemHeight: number;
  renderItem: (node: HTMLElement, index: number) => void;
  overscan?: number;
  onRangeChange?: (start: number, end: number) => void;
}

interface VirtualListAPI {
  updateItemCount: (newCount: number) => void;
  updateEstimatedHeight: (newHeight: number) => void;
  scrollToIndex: (index: number) => void;
  destroy: () => void;
}

/**
 * Initializes a virtual list within a container element.
 *
 * @param container - The scrollable container element
 * @param options - Configuration options
 * @returns API object with updateItemCount, scrollToIndex, destroy
 */
export function initVirtualList(
  container: HTMLElement,
  {
    itemCount,
    estimatedItemHeight,
    renderItem,
    overscan = 6,
    onRangeChange,
  }: VirtualListOptions
): VirtualListAPI {
  if (!container || typeof renderItem !== "function") {
    throw new Error(
      "initVirtualList requires a container and renderItem function"
    );
  }

  // State
  let currentItemCount = itemCount || 0;
  let currentEstimatedHeight = estimatedItemHeight || 100;
  let visibleStart = 0;
  let visibleEnd = 0;
  let isDestroyed = false;

  container.style.overflow = "auto";
  if (getComputedStyle(container).position === "static") {
    container.style.position = "relative";
  }

  // Create inner structure
  const spacer = document.createElement("div");
  spacer.style.position = "absolute";
  spacer.style.top = "0";
  spacer.style.left = "0";
  spacer.style.width = "1px";
  spacer.style.height = `${currentItemCount * currentEstimatedHeight}px`;
  spacer.style.pointerEvents = "none";
  container.appendChild(spacer);

  const itemsContainer = document.createElement("div");
  itemsContainer.style.position = "relative";
  itemsContainer.style.width = "100%";
  container.appendChild(itemsContainer);

  // Pool of recycled nodes
  const nodePool = [];
  let maxPoolSize =
    Math.ceil(container.clientHeight / currentEstimatedHeight) +
    overscan * 2 +
    5;

  function updateMaxPoolSize() {
    maxPoolSize =
      Math.ceil(container.clientHeight / currentEstimatedHeight) +
      overscan * 2 +
      5;
  }

  /**
   * Gets or creates a DOM node from the pool
   */
  function getNode() {
    if (nodePool.length > 0) {
      return nodePool.pop();
    }
    const node = document.createElement("div");
    node.style.position = "absolute";
    node.style.top = "0";
    node.style.left = "0";
    node.style.width = "100%";
    return node;
  }

  /**
   * Returns a node to the pool
   */
  function releaseNode(node) {
    if (nodePool.length < maxPoolSize * 2) {
      node.innerHTML = "";
      node.removeAttribute("data-index");
      node.className = "";
      nodePool.push(node);
    } else {
      node.remove();
    }
  }

  /**
   * Calculates the visible range based on scroll position
   */
  function calculateVisibleRange() {
    const scrollTop = container.scrollTop;
    const viewportHeight = container.clientHeight;

    const startIndex = Math.floor(scrollTop / currentEstimatedHeight);
    const endIndex = Math.ceil(
      (scrollTop + viewportHeight) / currentEstimatedHeight
    );

    const start = Math.max(0, startIndex - overscan);
    const end = Math.min(currentItemCount, endIndex + overscan);

    return { start, end };
  }

  /**
   * Renders the items in the visible range
   */
  function render() {
    if (isDestroyed) return;

    const { start, end } = calculateVisibleRange();

    // Skip if range hasn't changed
    if (start === visibleStart && end === visibleEnd) {
      return;
    }

    visibleStart = start;
    visibleEnd = end;

    // Collect existing nodes
    const existingNodes = Array.from(itemsContainer.children);
    const nodesToKeep = new Map();
    const nodesToRelease = [];

    // Identify which nodes to keep
    existingNodes.forEach((node) => {
      const index = parseInt(node.dataset.index, 10);
      if (index >= start && index < end) {
        nodesToKeep.set(index, node);
      } else {
        nodesToRelease.push(node);
      }
    });

    // Release unused nodes
    nodesToRelease.forEach((node) => {
      releaseNode(node);
      itemsContainer.removeChild(node);
    });

    // Render items in range
    for (let i = start; i < end; i++) {
      let node = nodesToKeep.get(i);

      if (!node) {
        node = getNode();
        node.dataset.index = String(i);
        itemsContainer.appendChild(node);
      }

      // Position the node
      node.style.transform = `translateY(${i * currentEstimatedHeight}px)`;

      // Render content
      renderItem(node, i);
    }

    // Notify range change
    if (onRangeChange) {
      onRangeChange(start, end);
    }
  }

  /**
   * Updates spacer height
   */
  function updateSpacerHeight() {
    spacer.style.height = `${currentItemCount * currentEstimatedHeight}px`;
  }

  /**
   * Handle scroll events
   */
  let scrollTimeout = null;
  function handleScroll() {
    if (scrollTimeout) {
      clearTimeout(scrollTimeout);
    }
    scrollTimeout = setTimeout(() => {
      render();
    }, 16); // ~60fps
  }

  // Attach scroll listener
  container.addEventListener("scroll", handleScroll);

  // Initial render
  render();

  // Public API
  return {
    /**
     * Updates the item count and re-renders
     */
    updateItemCount(newCount) {
      currentItemCount = newCount || 0;
      updateSpacerHeight();
      updateMaxPoolSize();
      render();
    },

    /**
     * Updates the estimated item height used for positioning
     */
    updateEstimatedHeight(newHeight) {
      if (!newHeight || !Number.isFinite(newHeight) || newHeight <= 0) {
        return;
      }
      currentEstimatedHeight = newHeight;
      updateSpacerHeight();
      updateMaxPoolSize();
      render();
    },

    /**
     * Scrolls to show a specific item
     */
    scrollToIndex(index) {
      if (index < 0 || index >= currentItemCount) return;
      container.scrollTop = index * currentEstimatedHeight;
      render();
    },

    /**
     * Cleans up and removes all event listeners
     */
    destroy() {
      if (isDestroyed) return;
      isDestroyed = true;

      container.removeEventListener("scroll", handleScroll);
      if (scrollTimeout) {
        clearTimeout(scrollTimeout);
      }

      // Clean up all nodes
      Array.from(itemsContainer.children).forEach((node) => {
        node.remove();
      });

      spacer.remove();
      itemsContainer.remove();
      nodePool.length = 0;
    },
  };
}
