let initialized = false;

interface Star {
  x: number;
  y: number;
  r: number;
  dx: number;
  dy: number;
}

export function initStarfield(): void {
  if (initialized) {
    return;
  }

  const canvas = document.getElementById(
    "starCanvas"
  ) as HTMLCanvasElement | null;
  if (!canvas) {
    return;
  }
  const canvasEl: HTMLCanvasElement = canvas;

  const maybeContext = canvasEl.getContext("2d");
  if (!maybeContext) {
    return;
  }
  const ctx: CanvasRenderingContext2D = maybeContext;

  const stars: Star[] = [];
  const numStars = 150;

  function resizeCanvas() {
    canvasEl.width = window.innerWidth;
    canvasEl.height = window.innerHeight;
  }

  window.addEventListener("resize", resizeCanvas);
  resizeCanvas();

  for (let i = 0; i < numStars; i += 1) {
    stars.push({
      x: Math.random() * canvasEl.width,
      y: Math.random() * canvasEl.height,
      r: Math.random() * 2 + 0.5,
      dx: (Math.random() - 0.5) * 0.3,
      dy: (Math.random() - 0.5) * 0.3,
    });
  }

  function animate() {
    ctx.clearRect(0, 0, canvasEl.width, canvasEl.height);
    ctx.fillStyle = "#fff";
    for (const star of stars) {
      ctx.beginPath();
      ctx.arc(star.x, star.y, star.r, 0, Math.PI * 2);
      ctx.fill();
      star.x += star.dx;
      star.y += star.dy;
      if (star.x < 0) star.x = canvasEl.width;
      if (star.x > canvasEl.width) star.x = 0;
      if (star.y < 0) star.y = canvasEl.height;
      if (star.y > canvasEl.height) star.y = 0;
    }
    requestAnimationFrame(animate);
  }

  animate();
  initialized = true;
}
