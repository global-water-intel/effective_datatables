// http://stackoverflow.com/questions/7373023/throttle-event-calls-in-jquery

(function($) {
  $.fn.delayedChange = function(options) {
    var timer; var o;

    if (jQuery.isFunction(options)) {
      o = { onChange: options };
    } else {
      o = options;
    }

    o = $.extend({}, $.fn.delayedChange.defaultOptions, o);

    return this.each(function() {
      var element = $(this);
      // If you load the page with some value already in a text filter,
      // then select the whole value and delete it
      // delayedChange doesn't recognize that a search needs to be performed
      // because it always initializes oldVal to ''.
      // Change it so that
      // a) We can initialize oldVal
      // b) oldVal is saved per HTML element rather than as a global
      element.delayedChange[element.prop('id')] = {};
      if (!element.delayedChange[element.prop('id')].initializedOldVal) {
        element.delayedChange[element.prop('id')].oldVal = o['oldValue'];
        element.delayedChange[element.prop('id')].initializedOldVal = true;
      }
      element.keyup(function() {
        clearTimeout(timer);
        timer = setTimeout(function() {
          var newVal = element.val();
          newVal = $.trim(newVal);
          if (element.delayedChange[element.prop('id')].oldVal != newVal) {
            element.delayedChange[element.prop('id')].oldVal = newVal;
            o.onChange.call(this, element);
          }
        }, o.delay);
      });
    });
  };

  $.fn.delayedChange.defaultOptions = {
    delay: 700,
    onChange: function(element) { }
  }

  $.fn.delayedChange.oldVal = '';

})(jQuery);
