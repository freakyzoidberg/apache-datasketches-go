package kll

import (
	"math"
	"testing"
)

/*
  @Test
  public void empty() {
    final KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance();
    sketch.update(Double.NaN); // this must not change anything
    assertTrue(sketch.isEmpty());
    assertEquals(sketch.getN(), 0);
    assertEquals(sketch.getNumRetained(), 0);
    try { sketch.getRank(0); fail(); } catch (SketchesArgumentException e) {}
    try { sketch.getMinItem(); fail(); } catch (SketchesArgumentException e) {}
    try { sketch.getMaxItem(); fail(); } catch (SketchesArgumentException e) {}
    try { sketch.getQuantile(0.5); fail(); } catch (SketchesArgumentException e) {}
    try { sketch.getQuantiles(new double[] {0}); fail(); } catch (SketchesArgumentException e) {}
    try { sketch.getPMF(new double[] {0}); fail(); } catch (SketchesArgumentException e) {}
    try { sketch.getCDF(new double[] {0}); fail(); } catch (SketchesArgumentException e) {}
    assertNotNull(sketch.toString(true, true));
    assertNotNull(sketch.toString());
  }
*/

func TestDoubleSketchEmpty(t *testing.T) {
	sketch := NewKllDoubleSketchWithDefault()
	sketch.Update(math.NaN()) // this must not change anything
}
