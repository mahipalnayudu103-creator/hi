# 📱 UI Responsive Improvement Plan

This document outlines the current responsive design limitations of **RenkoTerminal** and provides a roadmap for next-phase modifications to ensure premium look-and-feel across all devices (Mobile, Tablet, Desktop).

---

## 🔍 Current Screen Layout Issues

1. **Fixed Sidebar Width**: The left panel uses a fixed width (`width: 320px`), causing it to clip or occupy too much space on smaller desktop resolutions and tablets.
2. **Dense Multi-Column Parameters**: Grid inputs (e.g., Brick sizes grid, price options) overflow boundaries on narrower screens.
3. **Four-Chart Grid Scaling**: The 2×2 charts grid container (`.charts-grid`) behaves poorly below 1200px. High charts compress vertically, causing Legend labels to overlap.
4. **Console Log & Right Panel Density**: Right metrics panel and bottom Live console take up space that leaves the central charts grid cramped.

---

## 🗺️ Responsive Layout Strategy

We will update [responsive.css](file:///D:/renko_playback/frontend/static/css/responsive.css) and refine UI containers inside [index.html](file:///D:/renko_playback/frontend/index.html) to implement the following changes:

### 1. Large Screen Desktop ( > 1200px)
- Maintain standard 3-column layout: Sidebar (Left) | Main Viewport (Center) | Metrics & Measure (Right).
- Central dashboard displays 2×2 chart grid or single stacked viewport based on user's layout preferences.

### 2. Tablet Mode (768px - 1024px)
- **Sidebar Collapse**: Convert Left Sidebar to a collapsible menu or slide-out drawer (using a Hamburger button).
- **Chart Flex**: Switch `.charts-grid` to a 1-column stack or 2-column layout (maximum 2 charts wide).
- **Metrics Panel**: Move the right Metrics side panel into a toggleable overlay or merge it under the bottom Live Console.

### 3. Mobile Mode ( < 768px)
- **Top Panel Refactor**: Transform the system status monitor bar into a swipeable carousel or a simple summary chip bar to save vertical height.
- **Vertical Stack**: Stack all charts vertically in a single column. Force a minimum height of `320px` per chart.
- **Param Layouts**: Modify parameter grids inside the sidebar to list items vertically.
- **Bottom Console**: Collapse bottom Live Console into a minimized "Logs Badge" at the bottom of the screen.

---

## 🛠️ Next Steps for ChatGPT

To complete this responsive design, ChatGPT should carry out the following tasks:

1. **Collapsible left Sidebar**:
   - In `index.html`, wrap `<aside class="sidebar">` with toggle controls.
   - In `main.js`, add event listener to toggle `.sidebar-collapsed` class on body.
   - Add hover micro-animations to the toggle triggers.

2. **Refine `responsive.css`**:
   - Write media queries to redefine the grid layout properties:
     ```css
     @media (max-width: 1024px) {
         .app-container {
             grid-template-columns: 1fr;
         }
         .sidebar-right {
             position: fixed;
             right: -300px;
             transition: right 0.3s ease;
             z-index: 1000;
         }
         .sidebar-right.active {
             right: 0;
         }
     }
     ```

3. **Chart Resize Handler**:
   - Add window resize event debouncing in `charts.js` so TradingView charts adjust width/height dynamically when screens orientation changes:
     ```javascript
     window.addEventListener('resize', debounce(() => {
         charts.forEach(c => c.resize(container.clientWidth, container.clientHeight));
     }, 150));
     ```
