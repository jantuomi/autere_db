const iframe = document.createElement("iframe");
iframe.name = "htmp";
iframe.hidden = true;
document.body.appendChild(iframe);

document
  .querySelector("iframe[name='htmp']")
  .addEventListener("load", function () {
    setTimeout(() => {
      const cd = this.contentDocument;
      const cw = this.contentWindow;
      // If the server responds with an entire HTML document, replace the current document with it.
      // To check whether the response is an entire page we check the presence of this very snippet.
      if (cd.querySelector("iframe[name='htmp']")) {
        document.documentElement.replaceWith(cd.documentElement);
        // If the server responds with a fragment, replace the target element with it.
      } else {
        document
          .querySelector(cw.location.hash || null)
          ?.replaceWith(...cd.body.childNodes);
        const url = new URL(cw.location.href);
        url.searchParams.delete("htmp");
        url.hash = "";
        history.replaceState({}, "", url.toString());
      }
    });
  });

document.querySelectorAll("form[htmp]").forEach(function (el) {
  const re = el.attributes.replace.value;
  const input = document.createElement("input");
  input.type = "hidden";
  input.name = "htmp";
  input.value = re;
  el.appendChild(input);

  const action = new URL(el.action);
  action.hash = re;
  el.action = action.toString();
  el.target = "htmp";
});

document.querySelectorAll("a[htmp]").forEach(function (el) {
  const re = el.attributes.replace.value;

  const href = new URL(el.href);
  href.hash = re;
  href.searchParams.set("htmp", re);
  el.target = "htmp";
  el.href = href.toString();
});
