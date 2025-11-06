let initialized = false;

export function initStarfield() {
  if (initialized) {
    return;
  }
  const starCanvas = document.getElementById('starCanvas');
  if (!starCanvas || !starCanvas.getContext) {
    return;
  }
  const ctx = starCanvas.getContext('2d');
  const stars = [];
  const numStars = 150;

  function resizeCanvas() {
    starCanvas.width = window.innerWidth;
    starCanvas.height = window.innerHeight;
  }

  window.addEventListener('resize', resizeCanvas);
  resizeCanvas();

  for (let i = 0; i < numStars; i += 1) {
    stars.push({
      x: Math.random() * starCanvas.width,
      y: Math.random() * starCanvas.height,
      r: Math.random() * 2 + 0.5,
      dx: (Math.random() - 0.5) * 0.3,
      dy: (Math.random() - 0.5) * 0.3
    });
  }

  function animate() {
    ctx.clearRect(0, 0, starCanvas.width, starCanvas.height);
    ctx.fillStyle = '#fff';
    for (const star of stars) {
      ctx.beginPath();
      ctx.arc(star.x, star.y, star.r, 0, Math.PI * 2);
      ctx.fill();
      star.x += star.dx;
      star.y += star.dy;
      if (star.x < 0) star.x = starCanvas.width;
      if (star.x > starCanvas.width) star.x = 0;
      if (star.y < 0) star.y = starCanvas.height;
      if (star.y > starCanvas.height) star.y = 0;
    }
    requestAnimationFrame(animate);
  }

  animate();
  initialized = true;
}
