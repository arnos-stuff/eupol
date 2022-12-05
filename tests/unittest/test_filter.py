from eupol.download.stats.eu.tfilter import TopicFilter

tfncols = 7
# tf = TopicFilter(topic='population')

def test_init():
    # assert tf.relevant().name.to_list() ==
    topics = ['sex', 'age', 'nace', 'level','2']
    shapes = [(2053, tfncols), (2090, tfncols), (863, tfncols), (814, tfncols), (1652, tfncols)]
    for t,s in zip(topics, shapes):
        _tf = TopicFilter(topic=t)
        assert _tf.state.shape == s

def test_chain():
    tf = TopicFilter(topic="land")
    tf.filter("area")
    tf.filter("total")

    assert tf.state.shape == (1, tfncols)
    assert tf.state.iloc[0].code == "TGS00002"

def test_backtrack():
    tf = TopicFilter(topic="land")
    tf.filter("area")
    assert tf.shape == (21, tfncols)
    tf.backtrack()
    assert tf.shape == (136, tfncols)
    tf.filter("lmp")
    assert tf.shape == (8, tfncols)
    tf.filter("poland")
    assert tf.shape == (2, tfncols)
    tf.backtrack(2)
    assert tf.shape == (136, tfncols)

if __name__ == '__main__':
    # test_init()
    # test_chain()
    test_backtrack()