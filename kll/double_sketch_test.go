package kll

import (
	"github.com/stretchr/testify/assert"
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
	err := sketch.Update(math.NaN()) // this must not change anything
	assert.NoError(t, err)
	assert.True(t, sketch.IsEmpty())
	assert.Equal(t, sketch.GetN(), int64(0))
	assert.Equal(t, sketch.GetNumRetained(), 0)
	_, err = sketch.GetRank(0)
	assert.Error(t, err)
	_, err = sketch.GetMinItem()
	assert.Error(t, err)
	_, err = sketch.GetMaxItem()
	assert.Error(t, err)
	_, err = sketch.GetQuantile(0.5)
	assert.Error(t, err)
	_, err = sketch.GetQuantiles([]float64{0})
	assert.Error(t, err)
	_, err = sketch.GetPMF([]float64{0})
	assert.Error(t, err)
	_, err = sketch.GetCDF([]float64{0})
	assert.Error(t, err)
	assert.NotNil(t, sketch.String(true, true))
}

func TestGetQuantilesInvalidArg(t *testing.T) {
	sketch := NewKllDoubleSketchWithDefault()
	err := sketch.Update(1)
	assert.NoError(t, err)
	_, err = sketch.GetQuantile(-1.0)
	assert.Error(t, err)
}
